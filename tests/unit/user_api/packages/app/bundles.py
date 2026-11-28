from fidem import Bundle
from tests.unit.user_api.packages.shared.guards import shared_guard

# Discovery finds no bundles, so the guard of the bundle stays out of the application.
SHARED = Bundle(shared_guard)
