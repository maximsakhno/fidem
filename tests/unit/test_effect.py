import pytest

from tests.effects import TestInternalEffect


class TestEffectIter:
    def test_yields_effect_itself(self) -> None:
        effect = TestInternalEffect()

        yielded_effects = list(effect)

        assert yielded_effects == [effect]

    def test_returns_sent_result(self) -> None:
        generator = iter(TestInternalEffect())
        next(generator)

        with pytest.raises(StopIteration) as exc_info:
            generator.send("effect_result")

        assert exc_info.value.value == "effect_result"
