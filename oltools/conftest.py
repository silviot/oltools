def pytest_collection_modifyitems(items):
    for item in items:
        if "postgresql" in repr(item):
            item.add_marker("slow")
