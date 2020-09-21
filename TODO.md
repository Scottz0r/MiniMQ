# MiniMQ TODO

- [ ] Message needs to track actual length instead of relying on buffer size.
- [ ] Consumer need to put a message type and length when sending.
  - [ ] Update test client to print output.
  - [ ] Update producer to change output (maybe a guid in the message?)
- [ ] Consumer heartbeat mode to keep stuff alive.
- [ ] Need to monitor consumers to when a message is not ACK after a timeout.
- [ ] Buffer manager to reuse byte buffers on messages.
  - Memory Pool class. Also need to hold stuff as IMemoryOwner.

- [ ] Consider reworking Producer to be like Consumer (with a class and events.)
- [ ] Consider breaking Consumer state changes into separate events.
- [ ] Consider injecting the message manager in producer/consumer?
  - Is there actually any real need to do this? The implementation will never change for this specific project. No need to muddy things up, testing is neigh impossible with high socket I/O code.
