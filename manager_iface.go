package peering

type PeeringManager interface {
        Listen(addr string) error
        Dial(addr string) error
        Broadcast(msg []byte)
        BroadcastExcept(msg []byte, exceptPeerID string)
        Events() <-chan PeerEvent
        PeerCount() int
        Addr() string
        Stop()
}
