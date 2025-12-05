# verison 0.1 Not implemented ttl logic, Filtering using exact match (expressions not implemented)

import aerospike
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
        return (self.ns, self.set, {SEP}.join([*namespace, key]))
    
    def _put(self, key, bins: Dict[str, Any]) -> None:
        try:
            self.client.put(key, bins)
        except aerospike.exception.AerospikeError as e:
            raise RuntimeError(f"Aerospike put failed for {key}: {e}") from e
    
    def _scan_records(self):
        scan = self.client.scan(self.ns, self.set)
        records = scan.results()
        return [bins for (_k, _m, bins) in records]
    
    @staticmethod
    def _match_prefix(ns: tuple[str, ...], prefix: NamespacePath) -> bool:
        if len(ns) < len(prefix):
            return False
        length = len(prefix)
        i = 0
        while i < length:
            if prefix[i] == "*":
                i += 1
                continue
            if ns[i] != prefix[i]:
                return False
            i += 1
        return True
    
    @staticmethod
    def _match_suffix(ns: tuple[str, ...], suffix: NamespacePath) -> bool:
        if len(ns) < len(suffix):
            return False
        start_pointer = len(ns) - len(suffix)
        while start_pointer < len(suffix):
            if suffix[start_pointer] == "*":
                start_pointer += 1
                continue
            if ns[start_pointer] != suffix[start_pointer]:
                return False
            start_pointer += 1
        return True
    
    def write(self, op: PutOp):

        key = self._key(op.namespace, op.key)
        if op.value is None:
            try:
                self.client.remove(key)
            except aerospike.exception.AerospikeError as e:
                raise RuntimeError(f"Aerospike remove failed for {key}: {e}") from e
            return
        
        now = _now_utc()
        try:
            old_key, old_meta, old_bins = self.client.get(key)
            created_at = old_bins.get("created_at", now)
        except aerospike.exception.AerospikeError as e:
            created_at = now

        bins = {
            "namespace": list(op.namespace),
            "key": op.key,
            "value": op.value,
            "created_at": created_at,
            "updated_at": now,

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
        bins_list = self._scan_records()
        all_namespaces: list[tuple[str, ...]] = set()
        for bins in bins_list:
            ns = tuple(bins.get("namespace", ()))
            if prefix and not self._match_prefix(ns= ns, prefix=prefix):
                continue
            if suffix and not self._match_suffix(ns= ns, suffix= suffix):
                continue

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
        bins_list = self._scan_records()
        out: list[SearchItem] = []

        for bins in bins_list:
            ns = tuple(bins.get("namespace", ()))
            if not ns:
                continue
            if namespace_prefix and not self._match_prefix(ns, prefix= namespace_prefix):
                continue

            key = bins.get("key")
            value = bins.get("value")

            if key is None or value is None:
                continue

            if filter:
                include = True
                for f_key, f_value in filter.items():
                    if value.get(f_key) != f_value:
                        include = False
                        break
                if not include:
                    continue

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

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        """Execute multiple operations synchronously in a single batch.

        Args:
            ops: An iterable of operations to execute.

        Returns:
            A list of results, where each result corresponds to an operation in the input.
            The order of results matches the order of input operations.
        """
        for op in ops:
            result : list[Result] = []
            dedeup_puts: dict[tuple[tuple[str, ...]], PutOp] = {}

            if isinstance(op, GetOp):
                result.append(
                    self.get(
                        namespace=op.namespace, 
                        key=op.key
                    )
                )

            elif isinstance(op, PutOp):
                dedeup_puts[(op.namespace, op.key)] = op
                result.append(None)

            elif isinstance(op, SearchOp):
                result.append(
                    self.search(
                        namespace_prefix = op.namespace_prefix,
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
            
            for put_op in dedeup_puts.values():                             # Does bulk write here makes sense?? For Faster Write.
                self.write(put_op)

            return result



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

