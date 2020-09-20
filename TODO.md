# MiniMQ TODO

- [ ] Consumer need to put a message type and length when sending.
  - [ ] Update test client to print output.
  - [ ] Update producer to change output (maybe a guid in the message?)
- [ ] Consumer heartbeat mode to keep stuff alive.
- [ ] Buffer manager to reuse byte buffers on messages.


- [ ] Consider reworking Producer to be like Consumer (with a class and events.)
- [ ] Consider breaking Consumer state changes into separate events.
- [ ] Consider injecting the message manager in producer/consumer?
  - Is there actually any real need to do this? The implementation will never change for this specific project. No need to muddy things up, testing is neigh impossible with high socket I/O code.
