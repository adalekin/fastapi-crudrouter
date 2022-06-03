import typing

import pytest

from . import test_router

PotatoUrl = "/potato/"
CarrotUrl = "/carrot/"
basic_carrot = dict(length=1.2, color="Orange")
basic_potato = dict(thickness=0.24, mass=1.2, color="Brown", type="Russet")

INSERT_COUNT = 20
PAGINATION_SIZE = 10


@pytest.fixture(scope="class")
def insert_items(
    client,
    url: str = PotatoUrl,
    model: typing.Dict = None,
    count: int = INSERT_COUNT,
):
    model = model or basic_potato
    for i in range(count):
        test_router.test_post(
            client,
            url=url,
            model=model,
            expected_length=i + 1 if i + 1 < PAGINATION_SIZE else PAGINATION_SIZE,
            pagination_size=PAGINATION_SIZE,
        )


@pytest.fixture(scope="class")
def insert_carrots(client):
    for i in range(INSERT_COUNT):
        test_router.test_post(client, CarrotUrl, basic_carrot, expected_length=i + 1)


@pytest.fixture(scope="class")
def cleanup(client):
    yield
    client.delete(CarrotUrl)
    client.delete(PotatoUrl)


def get_expected_length(size, page, count: int = INSERT_COUNT):
    expected_length = size

    if page * size > count:
        expected_length = 0

    return expected_length


@pytest.mark.usefixtures("insert_carrots", "insert_items", "cleanup")
class TestPagination:
    @pytest.mark.parametrize("page", [1, 2, 5, 10, 20, 40])
    @pytest.mark.parametrize("size", [1, 5, 10])
    def test_pagination(self, client, size, page):
        test_router.test_get(
            client=client,
            url=PotatoUrl,
            params={"size": size, "page": page},
            expected_length=get_expected_length(size, page),
            pagination_size=size,
        )

    @pytest.mark.parametrize("page", [-1, "asdas", 3.23])
    def test_invalid_offset(self, client, page):
        res = client.get(PotatoUrl, params={"page": page})
        assert res.status_code == 422

    @pytest.mark.parametrize("size", [-1, 0, "asdas", 3.23])
    def test_invalid_limit(self, client, size):
        res = client.get(PotatoUrl, params={"size": size})
        assert res.status_code == 422

    def test_pagination_disabled(self, client):
        test_router.test_get(client, CarrotUrl, expected_length=INSERT_COUNT)

    @pytest.mark.parametrize("size", [2, 5, 10])
    def test_paging(self, client, size):
        ids = set()
        page = 1
        while page <= (int(INSERT_COUNT / size) + INSERT_COUNT % size):
            data = test_router.test_get(
                client,
                PotatoUrl,
                params={"size": size, "page": page},
                expected_length=get_expected_length(size, page),
                pagination_size=size,
            )

            for item in data["items"]:
                assert item["id"] not in ids
                ids.add(item["id"])

            page += 1

        assert len(ids) == INSERT_COUNT

    @pytest.mark.parametrize("size", [2, 5, 10])
    def test_paging_no_page(self, client, size):
        client.delete(PotatoUrl)
        for i in range(size):
            res = client.post(url=PotatoUrl, json=basic_potato)
            assert res.status_code == 201, res.json()

        res = client.get(PotatoUrl)
        assert res.status_code == 200, res.json()
        assert len(res.json()["items"]) == size

        res = client.get(PotatoUrl, params={"size": size})
        assert res.status_code == 200, res.json()
        assert len(res.json()["items"]) == size

        size = int(size / 2)
        res = client.get(PotatoUrl, params={"size": size})
        assert res.status_code == 200, res.json()
        assert len(res.json()["items"]) == size
