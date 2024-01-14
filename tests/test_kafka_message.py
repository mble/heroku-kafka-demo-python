from demo.kafka_message import KafkaMessage, KafkaMessageMetadata, MessageBuffer


class TestKafkaMessage:
    """Test KafkaMessage."""

    def test_to_dict(self):
        msg = KafkaMessage(
            value="test",
            partition=0,
            offset=0,
            metadata=KafkaMessageMetadata(receivedAt=0),
        )
        assert msg.to_dict() == {
            "value": "test",
            "partition": 0,
            "offset": 0,
            "metadata": {"receivedAt": 0},
        }


class TestMessageBuffer:
    """Test MessageBuffer."""

    def test_append(self):
        buffer = MessageBuffer(2)
        buffer.append(1)
        buffer.append(2)
        buffer.append(3)
        assert list(buffer) == [2, 3]

    def test_iter(self):
        buffer = MessageBuffer(2)
        buffer.append(1)
        buffer.append(2)
        for i, j in zip(buffer, [1, 2]):
            assert i == j
