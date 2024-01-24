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
        msgs: list[KafkaMessage] = []
        for i in range(3):
            msgs.append(
                KafkaMessage(
                    value=str(i),
                    partition=0,
                    offset=0,
                    metadata=KafkaMessageMetadata(receivedAt=0),
                )
            )

        buffer = MessageBuffer(2)
        for msg in msgs:
            buffer.append(msg)
        assert list(buffer) == [msgs[1], msgs[2]]

    def test_iter(self):
        msgs: list[KafkaMessage] = []
        for i in range(2):
            msgs.append(
                KafkaMessage(
                    value=str(i),
                    partition=0,
                    offset=0,
                    metadata=KafkaMessageMetadata(receivedAt=0),
                )
            )

        buffer = MessageBuffer(2)
        for msg in msgs:
            buffer.append(msg)
        for i, j in zip(buffer, [msgs[0], msgs[1]]):
            assert i == j
