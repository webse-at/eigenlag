import ast

from scanner.wrappers import definition_spans, resolve, scan_file


def names(*sources: str) -> frozenset[str]:
    return resolve([scan_file(ast.parse(source)) for source in sources])


def test_funktion_die_einen_dag_zurueckgibt_ist_konstruktor() -> None:
    assert "make_dag" in names("def make_dag(x):\n    return DAG(x)\n")


def test_methode_die_einen_dag_zurueckgibt_ist_konstruktor() -> None:
    source = """
class Factory:
    def create_easy_dag(self, dag_id, **kwargs):
        return DAG(dag_id, **kwargs)
"""
    assert "create_easy_dag" in names(source)


def test_alias_auf_einen_konstruktor_zaehlt_mit() -> None:
    # Wikimedias Muster: die gebundene Methode wandert unter neuem Namen ins Modul.
    source = """
class Factory:
    def create_easy_dag(self, dag_id):
        return DAG(dag_id)

EasyDag = Factory().create_easy_dag
"""
    assert names(source) >= {"create_easy_dag", "EasyDag"}


def test_alias_ueber_file_grenzen_hinweg() -> None:
    factory = "class F:\n    def create(self, i):\n        return DAG(i)\n"
    binding = "from f import F\n\nEasyDag = F().create\n"
    assert names(factory, binding) >= {"create", "EasyDag"}


def test_helfer_ohne_dag_rueckgabe_ist_kein_konstruktor() -> None:
    assert names("def build_operator(t):\n    return BashOperator(task_id=t)\n") == frozenset()


def test_return_einer_verschachtelten_funktion_zaehlt_nicht_fuer_die_aeussere() -> None:
    source = """
def outer():
    def inner():
        return DAG("x")
    return inner
"""
    assert names(source) == frozenset({"inner"})


def test_weiterreichen_macht_noch_keinen_konstruktor() -> None:
    # ADR-015, Punkt 3: build_dag ruft den Konstruktor nur auf, der DAG entsteht dort.
    source = """
def build_dag(dag_id):
    with create_easy_dag(dag_id=dag_id) as dag:
        pass
    return dag
"""
    assert names(source) == frozenset()


def test_spans_decken_den_rumpf_des_konstruktors() -> None:
    tree = ast.parse("def make_dag(x):\n    with DAG(x) as d:\n        pass\n    return d\n")
    assert definition_spans(tree, frozenset({"make_dag"})) == [(1, 4)]
    assert definition_spans(tree, frozenset({"DAG"})) == []
