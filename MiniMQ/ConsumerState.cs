namespace MiniMQ
{
    public enum ConsumerState
    {
        Ready,
        WaitingResponse,
        WaitingHeartbeat,
        Closed
    }
}
