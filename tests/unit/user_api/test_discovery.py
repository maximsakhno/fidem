import pytest

from fidem._discovery import discover
from tests.unit.user_api.packages import app, broken, shared
from tests.unit.user_api.packages.app.billing.charges import charge
from tests.unit.user_api.packages.app.bundles import SHARED
from tests.unit.user_api.packages.app.messages import ChargeCommand, ChargedEvent, RefundedEvent
from tests.unit.user_api.packages.shared import guards
from tests.unit.user_api.packages.shared.guards import shared_guard


class TestDiscover:
    def test_finds_declarations_of_submodules_of_package(self) -> None:
        discovered = list(discover([app]))

        assert set(map(id, discovered)) == {id(charge), id(ChargedEvent), id(RefundedEvent)}

    def test_skips_command_class(self) -> None:
        assert ChargeCommand not in list(discover([app]))

    def test_skips_declaration_imported_from_module_it_does_not_scan(self) -> None:
        assert shared_guard not in list(discover([app]))

    def test_skips_bundle(self) -> None:
        assert SHARED not in list(discover([app]))

    def test_finds_declaration_imported_from_module_it_scans_too(self) -> None:
        assert shared_guard in list(discover([app, shared]))

    def test_scans_only_module_that_is_not_package(self) -> None:
        assert list(discover([guards])) == [shared_guard]

    def test_raises_import_error_of_submodule(self) -> None:
        with pytest.raises(ImportError, match="failing module"):
            list(discover([broken]))
