namespace EventReader.Kafka;

public sealed class EventReaderProcessingException : Exception
{
    public EventReaderProcessingException(string message)
        : base(message)
    {
    }

    public EventReaderProcessingException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
