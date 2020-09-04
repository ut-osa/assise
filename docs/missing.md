
### Missing Features ###

- IP addresses and interface names for replicas are hardcoded.
- Assise can't be flexibly configured in runtime. We should introduce config files for LibFS/KernFS.
	* Most options are defined via preprocessor flags (e.g. USE_LEASE) or variables (g_n_nodes, g_log_size), which necessitates re-compilation.
	* Others rely on environment variables (e.g. PORTNO, DEVID, etc.), which is not ideal (as applications may have conflicting environment variables).
- No configuration flags for reserve replicas.
- MR registration is performed on the granularity of dax devices. Refactor to only register memory segments being used. **[In-progress: Waleed]**
- Coalescing implementation in the replication path is currently deprecated and needs refactoring. **[In-progress: Waleed]**
- Log/shared areas are mmap’d from devdax devices and are consequently not private. Need a mechanism for enforcing access controls.
- ~~Assise can either busy-wait (cpu-intensive) or use signaling (slower) when polling for RPCs. Both are situational. Implement a middle-ground approach that automatically switches from busy-waiting to signaling after a timeout. **[In-progress: Waleed]**~~


### Known Issues / Bugs ###
- Filebench runtime error (Seems to have already been fixed on replication-offload branch).
- ~~Assise still has dependencies on RDMA even when configured as a local filesystem. **[In-progress: Waleed]** ~~
