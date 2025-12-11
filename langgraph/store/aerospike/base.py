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
    MatchCondition,
    NOT_PROVIDED,
    NotProvided
)
from collections.abc import Iterable
from typing import (
    Optional,
    Dict,
    Any,
    Literal
)
import time
# Initializing params and helper Methods

SEP = "|"


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)

# Base class

class AerospikeStore(BaseStore):
    supports_ttl: bool = True
    def __init__(
        self,
        client : aerospike.Client,
        namespace : str = "langgraph",
        set : str = "store",
        ttl_config: Optional[TTLConfig] = None
    ) -> None:
        self.client = client
        self.ns = namespace
        self.set = set
        self.ttl_config = ttl_config
    
    # --------------- Helper Functions ------------------
    def _key(self, namespace: tuple[str, ...], key: str) -> str:
        return (self.ns, self.set, SEP.join([*namespace, key]))
    
    def _put(self, key, bins: Dict[str, Any], ttl: Optional[int]) -> None:
        try:
            if ttl is not None:
                self.client.put(key, bins, {"ttl": ttl})
            else:
                self.client.put(key, bins)
        except aerospike.exception.AerospikeError as e:
            raise RuntimeError(f"Aerospike put failed for {key}: {e}") from e
        
    def _build_read_policy_for_refresh(self, refresh_ttl: bool | None) -> dict[str, Any]:
        policy: dict[str, Any] = {}
        if self.ttl_config is not None and self.ttl_config.get("refresh_on_read"):
            policy["read_touch_ttl_percent"] = 100
        if refresh_ttl:
            policy["read_touch_ttl_percent"] = 100
        return policy
    
    def _get_type_result(self, value: Any):
        """Helper to determine the Aerospike ResultType based on Python value type."""
        if isinstance(value, bool):
            return exp.ResultType.BOOLEAN
        elif isinstance(value, int):
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
    
    def _get_op_expression(self, bin_expr, value_expr, operator: str):
        ops = {
            "$eq": exp.Eq,
            "$ne": exp.NE,
            "$gt": exp.GT,
            "$gte": exp.GE,
            "$lt": exp.LT,
            "$lte": exp.LE,
        }
        
        if operator not in ops:
            raise ValueError(f"Unsupported operator: {operator}")
            
        return ops[operator](bin_expr, value_expr)
    
    def _build_path_filter(self, path: NamespacePath, bin_name: str, is_suffix: bool = False) -> list:
        """
        Builds a list of expressions to handle wildcards in NamespacePath.
        """
        conditions = []
        path_len = len(path)
        size_check = exp.GE(
            exp.ListSize(None, exp.ListBin(bin_name)), 
            exp.Val(path_len)
        )
        conditions.append(size_check)
        for i, token in enumerate(path):
            if token == "*":
                continue
            if is_suffix:
                algo_index = i - path_len
            else:
                algo_index = i
            result_type = self._get_type_result(token)
            match_condition = exp.Eq(
                exp.ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, result_type, exp.Val(algo_index), exp.ListBin(bin_name)),
                exp.Val(token)
            )
            conditions.append(match_condition)
            
        return conditions
    
    def _build_filter_exprs_from_dict(self, filter_dict: dict[str, Any]) -> list:
        filter_exprs = []
        
        for key, condition in filter_dict.items():
            map_key_expr = exp.Val(key)
            if isinstance(condition, dict) and any(k.startswith("$") for k in condition.keys()):
                
                for op, val in condition.items():
                    result_type = self._get_type_result(val)
                    target_expr = exp.MapGetByKey(
                        None, 
                        aerospike.MAP_RETURN_VALUE, 
                        result_type, 
                        map_key_expr, 
                        exp.MapBin("value")
                    )
                    
                    op_expr = self._get_op_expression(target_expr, exp.Val(val), op)
                    filter_exprs.append(op_expr)
            
            else:
                result_type = self._get_type_result(condition)
                target_expr = exp.MapGetByKey(
                    None, 
                    aerospike.MAP_RETURN_VALUE, 
                    result_type, 
                    map_key_expr, 
                    exp.MapBin("value")
                )
                filter_exprs.append(exp.Eq(target_expr, exp.Val(condition)))
                
        return filter_exprs
    
    # --------------- Base Functions --------------------
    
    def put(
        self,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any],
        index: Literal[False] | list[str] | None = None,
        *,
        ttl: float | None | NotProvided = NOT_PROVIDED,
    ) -> None:
        p_key = self._key(namespace, key)
        if value is None:
            try:
                self.client.remove(p_key)
            except aerospike.exception.AerospikeError as e:
                raise RuntimeError(f"Aerospike remove failed for {key}: {e}") from e
            return
        
        now = _now_utc().isoformat()
        try:
            _, _, old_bins = self.client.get(p_key)
            created_at = old_bins.get("created_at", now)
        except aerospike.exception.AerospikeError as e:
            created_at = now
        
        time_to_live: Optional[int] = None
        if ttl is None:
            time_to_live = -1
        elif ttl is not NOT_PROVIDED:
            if ttl < 0:
                time_to_live = -1
            else: time_to_live = int(ttl * 60)
        else:
            if self.ttl_config is not None:
                default_ttl_minutes = self.ttl_config.get("default_ttl", None)
                if default_ttl_minutes is not None:
                    time_to_live = int(default_ttl_minutes * 60)
            

        bins = {
            "namespace": list(namespace),
            "key": key,
            "value": value,
            "created_at": created_at,
            "updated_at": now,

        }                                                                           # Should we append here or just update
        self._put(p_key, bins, time_to_live)
        
    
    # def write(self, op: PutOp):

    #     key = self._key(op.namespace, op.key)
    #     if op.value is None:
    #         try:
    #             self.client.remove(key)
    #         except aerospike.exception.AerospikeError as e:
    #             raise RuntimeError(f"Aerospike remove failed for {key}: {e}") from e
    #         return
        
    #     now = _now_utc().isoformat()
    #     try:
    #         _, _, old_bins = self.client.get(key)
    #         created_at = old_bins.get("created_at", now)
    #     except aerospike.exception.AerospikeError as e:
    #         created_at = now
        
    #     ttl: Optional[int] = None
    #     if self.ttl_config is not None:
    #         default_ttl_minutes = self.ttl_config.get("default_ttl", None)
    #         if default_ttl_minutes is not None:
    #             ttl = int(default_ttl_minutes * 60)
    #     if op.ttl is not None:
    #         if op.ttl < 0:
    #             ttl = -1
    #         else: ttl = int(op.ttl * 60)

    #     bins = {
    #         "namespace": list(op.namespace),
    #         "key": op.key,
    #         "value": op.value,
    #         "created_at": created_at,
    #         "updated_at": now,

    #     }                                                                           # Should we append here or just update
    #     self._put(key, bins, ttl)

    def get(
        self, 
        namespace: tuple[str, ...], 
        key: str,
        *,
        refresh_ttl: bool | None = None,
    ) -> Item | None:
        pkey = self._key(namespace= namespace, key= key)
        
        read_policy = self._build_read_policy_for_refresh(refresh_ttl)
        try:
            if read_policy:
                _, _, bins = self.client.get(pkey, policy= read_policy)
            else:
                _, _, bins = self.client.get(pkey)
        except aerospike.exception.AerospikeError as e:
            return None
        
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
        
        policy = {}
        if filter_exprs:
            final_expr = exp.And(*filter_exprs)
            policy["expressions"] = final_expr.compile()
        try:
            scan = self.client.scan(self.ns, self.set)
            records = scan.results(policy= policy)
            print(f"Length of records {len(records)}")
        except aerospike.exception.AerospikeError as e:
            raise RuntimeError(f"Aerospike search failed: {e}") from e
        
        all_namespaces: list[tuple[str, ...]] = set()
        for _, _, bins in records:
            ns = tuple(bins.get("namespace", ()))
            if max_depth is not None:
                ns = ns[: max_depth]
            all_namespaces.add(ns)

        all_namespaces_list = list(all_namespaces)
        if offset:
            all_namespaces_list = all_namespaces_list[offset:]
        if limit:
            all_namespaces_list = all_namespaces_list[: limit]
        return all_namespaces_list
    
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
            filter_conditions = self._build_filter_exprs_from_dict(filter)
            filter_exprs.extend(filter_conditions)

        policy = {}
        if filter_exprs:
            final_expr = exp.And(*filter_exprs)
            policy["expressions"] = final_expr.compile()
        
        try:
            scan = self.client.scan(self.ns, self.set)
            records = scan.results(policy=policy)
        except aerospike.exception.AerospikeError as e:
            raise RuntimeError(f"Aerospike search failed: {e}") from e
        
        read_policy = self._build_read_policy_for_refresh(refresh_ttl)
        out: list[SearchItem] = []
        

        for _, _, bins in records:
            if read_policy:
                try:
                    _, _, bins = self.client.get(key, policy=read_policy)                  # Is there any better way??
                except aerospike.exception.AerospikeError:
                    continue
            ns = tuple(bins.get("namespace", ()))
            key = bins.get("key")
            value = bins.get("value")
            created_at = bins.get("created_at", _now_utc())
            updated_at = bins.get("updated_at", _now_utc())

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

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        """Execute multiple operations synchronously in a single batch.

        Args:
            ops: An iterable of operations to execute.

        Returns:
            A list of results, where each result corresponds to an operation in the input.
            The order of results matches the order of input operations.
        """
        result : list[Result] = []
        # dedeup_puts: dict[tuple[tuple[str, ...]], PutOp] = {}
        for op in ops:
            if isinstance(op, GetOp):
                result.append(
                    self.get(
                        namespace=op.namespace, 
                        key=op.key
                    )
                )

            elif isinstance(op, PutOp):
                #dedeup_puts[(op.namespace, op.key)] = op
                #self.write(op=op)
                self.put(
                    namespace= op.namespace,
                    key= op.key,
                    value= op.value,
                    ttl= op.ttl
                )
                result.append(None)

            elif isinstance(op, SearchOp):
                result.append(
                    self.search(
                        op.namespace_prefix,
                        filter= op.filter,
                        limit= op.limit,
                        offset= op.offset
                    )
                )

            elif isinstance(op, ListNamespacesOp):
                prefix = None
                suffix = None
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
                result.append(
                    self.list_namespaces(
                        prefix= prefix,
                        suffix= suffix,
                        max_depth= op.max_depth,
                        limit= op.limit,
                        offset= op.offset
                    )
                )
            else:
                raise TypeError(f'Unsupported operation type: {type(op)}')
            
        # for put_op in dedeup_puts.values():                             # Does bulk write here makes sense?? For Faster Write.
        #     self.put(
        #         namespace= put_op.namespace,
        #         key= put_op.key,
        #         value= put_op.value,
        #         ttl= put_op.ttl
        #     )

        return result



    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        """Execute multiple operations asynchronously in a single batch.

        Args:
            ops: An iterable of operations to execute.

        Returns:
            A list of results, where each result corresponds to an operation in the input.
            The order of results matches the order of input operations.
        """

        # Should use Aerospike batch ops
        pass
    pass

