# verison 0.1 Not implemented ttl logic, Filtering using exact match (expressions not implemented)

import aerospike
from aerospike_helpers import expressions as exp
from datetime import datetime, timezone
from langgraph.store.base import (
    BaseStore,
    GetOp,
    Item,
    ListNamespacesOp,
    NamespacePath,
    Op,
    PutOp,
    Result,
    SearchItem,
    SearchOp,
    TTLConfig,
    MatchCondition
)
from collections.abc import Iterable
from typing import (
    Optional,
    Dict,
    Any
)
from aerospike_helpers import expressions as exp
from langgraph.store.base import NamespacePath
# Initializing params and helper Methods

SEP = "|"

def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)

# Base class

class AerospikeStore(BaseStore):
    supports_ttl: bool = False
    def __init__(
        self,
        client : aerospike.Client,
        namespace : str = "langgraph",
        set : str = "store",
        ttl: Optional[int] = None
    ) -> None:
        self.client = client
        self.ns = namespace
        self.set = set
    
    # --------------- Helper Functions ------------------
    def _key(self, namespace: tuple[str, ...], key: str) -> str:
        return (self.ns, self.set, SEP.join([*namespace, key]))
    
    def _put(self, key, bins: Dict[str, Any]) -> None:
        try:
            self.client.put(key, bins)
        except aerospike.exception.AerospikeError as e:
            raise RuntimeError(f"Aerospike put failed for {key}: {e}") from e
    
    def _get_type_result(self, value: Any):
        """Helper to determine the Aerospike ResultType based on Python value type."""
        if isinstance(value, int):
            return exp.ResultType.INTEGER
        elif isinstance(value, float):
            return exp.ResultType.FLOAT
        elif isinstance(value, str):
            return exp.ResultType.STRING
        elif isinstance(value, bytes):
            return exp.ResultType.BLOB
        elif isinstance(value, (dict, list)):
            return exp.ResultType.MAP if isinstance(value, dict) else exp.ResultType.LIST
        return exp.ResultType.STRING
    
    def _build_path_filter(
        self,
        path: NamespacePath,
        bin_name: str,
        is_suffix: bool = False,
    ) -> list:
        """
        Builds a list of expressions to handle wildcards in NamespacePath.
        For example, prefix/suffix matching on the 'namespace' bin.
        """
        conditions: list = []
        path_len = len(path)

        # Require: len(namespace) >= len(path)
        size_check = exp.GE(  # <- was Ge
            exp.ListSize(None, exp.ListBin(bin_name)),
            exp.Val(path_len),
        )
        conditions.append(size_check)

        # For each element in the prefix/suffix, ensure equality
        for idx, part in enumerate(path):
            # Position from start or end
            pos = -(idx + 1) if is_suffix else idx

            elem_expr = exp.Eq(  # <- was Eq in some versions
                exp.ListGetByIndex(
                    None,
                    aerospike.LIST_RETURN_VALUE,
                    exp.ResultType.STRING,
                    exp.Val(pos),
                    exp.ListBin(bin_name),
                ),
                exp.Val(part),
            )
            conditions.append(elem_expr)

        return conditions
    # --------------- Core Functions --------------------
    
    def write(self, op: PutOp):

        key = self._key(op.namespace, op.key)
        if op.value is None:
            try:
                self.client.remove(key)
            except aerospike.exception.AerospikeError as e:
                raise RuntimeError(f"Aerospike remove failed for {key}: {e}") from e
            return
        
        now = _now_utc()
        now_str = _now_utc().isoformat()
        try:
            _, _, old_bins = self.client.get(key)
            # If an old created_at exists, keep it; otherwise use now_str
            created_at = old_bins.get("created_at") or now_str
        except aerospike.exception.AerospikeError:
            created_at = now_str

        bins = {
            "namespace": list(op.namespace),
            "key": op.key,
            "value": op.value,
            "created_at": created_at,
            "updated_at": now_str,

        }
        self._put(key, bins)

    # --------------- Base Functions --------------------
    def get(self, namespace: tuple[str, ...], key: str) -> Item | None:
        pkey = self._key(namespace= namespace, key= key)
        try:
            _, _, bins = self.client.get(pkey)
        except aerospike.exception.AerospikeError as e:
            raise RuntimeError(f"Aerospike get failed for {key}: {e}") from e
        
        ns = tuple(bins.get("namespace", namespace))
        k = bins.get("key", key)
        value = bins.get("value")
        if value is None:
            return None
        
        created_at = bins.get("created_at", _now_utc().isoformat())
        updated_at = bins.get("updated_at", _now_utc().isoformat())
        
        return Item(
            value= value,
            key= k,
            namespace= ns,
            created_at= created_at,
            updated_at= updated_at
        )

    def list_namespaces(
        self,
        *,
        prefix: NamespacePath | None = None,
        suffix: NamespacePath | None = None,
        max_depth: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[tuple[str, ...]]:
        """List and filter namespaces in the store.

        Used to explore the organization of data,
        find specific collections, or navigate the namespace hierarchy.

        Args:
            prefix: Filter namespaces that start with this path.
            suffix: Filter namespaces that end with this path.
            max_depth: Return namespaces up to this depth in the hierarchy.
                Namespaces deeper than this level will be truncated.
            limit: Maximum number of namespaces to return.
            offset: Number of namespaces to skip for pagination.

        Returns:
            A list of namespace tuples that match the criteria. Each tuple represents a
                full namespace path up to `max_depth`.

        ???+ example "Examples":

            Setting `max_depth=3`. Given the namespaces:

            ```python
            # Example if you have the following namespaces:
            # ("a", "b", "c")
            # ("a", "b", "d", "e")
            # ("a", "b", "d", "i")
            # ("a", "b", "f")
            # ("a", "c", "f")
            store.list_namespaces(prefix=("a", "b"), max_depth=3)
            # [("a", "b", "c"), ("a", "b", "d"), ("a", "b", "f")]
            ```
        """
        filter_exprs = []
        if prefix:
            prefix_conditions = self._build_path_filter(prefix, "namespace", is_suffix=False)
            filter_exprs.extend(prefix_conditions)
        if suffix:
            suffix_conditions = self._build_path_filter(suffix, "namespace", is_suffix=True)
            filter_exprs.extend(suffix_conditions)
        
        policy: dict[str, Any] = {}
        if filter_exprs:
            final_expr = exp.And(*filter_exprs)
            policy["filter_expression"] = final_expr.compile()

        try:
            scan = self.client.scan(self.ns, self.set)
            records = scan.results(policy=policy)
            # if policy:
            #     records = scan.results(policy)
            # else:
            #     records = scan.results()
        except aerospike.exception.AerospikeError as e:
            raise RuntimeError(f"Aerospike list_namespaces failed: {e}") from e

        all_namespaces: set[tuple[str, ...]] = set()
        for _, _, bins in records:
            ns_list = bins.get("namespace")
            if not ns_list:
                continue

            ns = tuple(ns_list)

            # apply max_depth truncation
            if max_depth is not None:
                ns = ns[:max_depth]

            # (Safety) re-apply prefix/suffix checks in Python
            if prefix is not None and ns[: len(prefix)] != prefix:
                continue
            if suffix is not None and ns[-len(suffix) :] != suffix:
                continue

            all_namespaces.add(ns)

        # ---- 4. Convert to list, sort, and slice for offset/limit ----
        ns_list = sorted(all_namespaces)

        if offset:
            ns_list = ns_list[offset:]
        if limit is not None:
            ns_list = ns_list[:limit]

        return ns_list
    
    def search(
        self,
        namespace_prefix: tuple[str, ...],
        /,
        *,
        query: Optional[str] = None,
        filter: Optional[dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0,
        refresh_ttl: Optional[bool] = None,
        **kwargs: Any,
    ) -> list[SearchItem]:                                    # Not taken Filter expressions into consideration
        if query:
            raise NotImplementedError(
                "Aerospike v0.1 does not support semantic/vector search. "
                "Use search without query. "
            )
        
        filter_exprs = []
        if namespace_prefix:
            prefix_conditions = self._build_path_filter(namespace_prefix, "namespace", is_suffix=False)
            filter_exprs.extend(prefix_conditions)

        if filter:
            for key, val in filter.items():
                result_type = self._get_type_result(val)
                expr_map_check = exp.Eq(
                    exp.MapGetByKey(None, aerospike.MAP_RETURN_VALUE, result_type, key, exp.MapBin("value")),
                    exp.Val(val)
                )
                filter_exprs.append(expr_map_check)
        policy = {}
        if filter_exprs:
            final_expr = exp.And(*filter_exprs)
            policy["filter_expression"] = final_expr.compile()
        
        try:
            scan = self.client.scan(self.ns, self.set)
            records = scan.results(policy=policy)
        except aerospike.exception.AerospikeError as e:
            raise RuntimeError(f"Aerospike search failed: {e}") from e
        
        out: list[SearchItem] = []
        

        for _, _, bins in records:
            ns = tuple(bins.get("namespace", ()))
            key = bins.get("key")
            value = bins.get("value")
            created_at = bins.get("created_at", _now_utc().isoformat())
            updated_at = bins.get("updated_at", _now_utc().isoformat())

            out.append(SearchItem(
                namespace= ns,
                key= key,
                value= value,
                created_at= created_at,
                updated_at= updated_at,
                score= None
            ))

        if offset:
            out = out[offset:]
        if limit is not None:
            out = out[:limit]

        return out

    # def batch(self, ops: Iterable[Op]) -> list[Result]:
    #     """Execute multiple operations synchronously in a single batch.

    #     Args:
    #         ops: An iterable of operations to execute.

    #     Returns:
    #         A list of results, where each result corresponds to an operation in the input.
    #         The order of results matches the order of input operations.
    #     """
    #     result: list[Result] = []
    #     dedeup_puts: dict[tuple[NamespacePath, str], PutOp] = {}

    #     for op in ops:
    #         # result : list[Result] = []
    #         # dedeup_puts: dict[tuple[tuple[str, ...]], PutOp] = {}

    #         if isinstance(op, GetOp):
    #             result.append(
    #                 self.get(
    #                     namespace=op.namespace, 
    #                     key=op.key
    #                 )
    #             )

    #         elif isinstance(op, PutOp):
    #             dedeup_puts[(op.namespace, op.key)] = op
    #             result.append(None)

    #         elif isinstance(op, SearchOp):
    #             result.append(
    #                 self.search(op)
    #                 # self.search(
    #                 #     namespace_prefix = op.namespace_prefix,
    #                 #     filter= op.filter,
    #                 #     limit= op.limit,
    #                 #     offset= op.offset
    #                 # )
    #             )

    #         elif isinstance(op, ListNamespacesOp):
    #             prefix: NamespacePath | None = None
    #             suffix: NamespacePath | None = None
    #             if op.match_conditions:
    #                 for condition in op.match_conditions:
    #                     if condition.match_type == "prefix":
    #                         prefix = condition.path
    #                     elif condition.match_type == "suffix":
    #                         suffix = condition.path
    #                     else:
    #                         raise ValueError(
    #                             f"Match type {condition.match_type} must be prefix or suffix."
    #                         )
    #             result.append(
    #                 self.list_namespaces(
    #                     prefix= prefix,
    #                     suffix= suffix,
    #                     max_depth= op.max_depth,
    #                     limit= op.limit,
    #                     offset= op.offset
    #                 )
    #             )
    #         else:
    #             raise TypeError(f'Unsupported operation type: {type(op)}')
            
    #     for put_op in dedeup_puts.values():                             # Does bulk write here makes sense?? For Faster Write.
    #         self.write(put_op)

    #     return result
    def batch(self, ops: Iterable[Op]) -> list[Result]:
        """Execute a batch of operations sequentially.

        Returns a list of results aligned with `ops`:
        - PutOp -> None
        - GetOp -> Item | None
        - SearchOp -> list[SearchItem]
        - ListNamespacesOp -> list[NamespacePath]
        """
        results: list[Result] = []

        for op in ops:
            if isinstance(op, PutOp):
                # Write immediately so later ops see it
                self.write(op)
                results.append(None)

            elif isinstance(op, GetOp):
                item = self.get(op.namespace, op.key)
                results.append(item)

            elif isinstance(op, SearchOp):
                search_results = self.search(op)
                results.append(search_results)

            elif isinstance(op, ListNamespacesOp):
                prefix: NamespacePath | None = None
                suffix: NamespacePath | None = None

                if op.match_conditions:
                    for condition in op.match_conditions:
                        if condition.match_type == "prefix":
                            prefix = condition.path
                        elif condition.match_type == "suffix":
                            suffix = condition.path
                        else:
                            raise ValueError(
                                f"Match type {condition.match_type} must be prefix or suffix."
                            )

                ns_list = self.list_namespaces(
                    prefix=prefix,
                    suffix=suffix,
                    max_depth=op.max_depth,
                    limit=op.limit,
                    offset=op.offset,
                )
                results.append(ns_list)

            else:
                raise TypeError(f"Unsupported operation type: {type(op)}")

        return results


    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        """Execute multiple operations asynchronously in a single batch.

        Args:
            ops: An iterable of operations to execute.

        Returns:
            A list of results, where each result corresponds to an operation in the input.
            The order of results matches the order of input operations.
        """
        pass
    pass