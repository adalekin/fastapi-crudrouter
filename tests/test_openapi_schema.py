from pytest import mark

from tests import CUSTOM_TAGS

PATHS = ["/potato/", "/carrot/"]
PATH_TAGS = {
    "/potato/": None,
    "/potato/{item_id}/": None,
    "/carrot/": CUSTOM_TAGS,
    "/carrot/{item_id}/": CUSTOM_TAGS,
}


class TestOpenAPISpec:
    def test_schema_exists(self, client):
        res = client.get("/openapi.json")
        assert res.status_code == 200

        return res

    def test_schema_tags(self, client):
        schema = self.test_schema_exists(client).json()
        paths = schema["paths"]

        assert len(paths) == len(PATH_TAGS)
        for path, method in paths.items():
            assert len(method) == 3

            for m in method:
                if PATH_TAGS[path]:
                    assert method[m]["tags"] == PATH_TAGS[path]

    @mark.parametrize("path", PATHS)
    def test_response_types(self, client, path):
        schema = self.test_schema_exists(client).json()
        paths = schema["paths"]

        for method in ["get", "post", "delete"]:
            assert "200" in paths[path][method]["responses"]

        assert "422" in paths[path]["post"]["responses"]

        item_path = path + "{item_id}/"
        for method in ["get", "patch", "delete"]:
            assert "200" in paths[item_path][method]["responses"]
            assert "404" in paths[item_path][method]["responses"]
            assert "422" in paths[item_path][method]["responses"]
