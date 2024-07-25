# MSPC IPC
Multi Producer Single Consumer for Inter-Process Communication

All ring-buffer implementations I was able to find assume they operate in a single process. When working 
across processes, the implementation must take into account that processes (producers or the consumer) 
can crash at any point, and the consumer must be able to recover and potentially skip broken entries.

One possibility is to use a robust futex, but that requires `pthreads`. In my scenario, I'm unable to use 
pthreads and reimplementing the logic there is too brittle.
