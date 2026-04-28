from fidem.intents import ask
from tests.intents import TestReadIntent


class TestAsk:
    def test_yeilds_passed_intent(self) -> None:
        intent = TestReadIntent()

        yeilded_intents = list(ask(intent))

        assert yeilded_intents == [intent]
