# verison 0.1 Not implemented ttl logic

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
        

        pass

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
                result.append(self._search(op))

            elif isinstance(op, ListNamespacesOp):
                result.append(self._listnamespaces(op))
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

