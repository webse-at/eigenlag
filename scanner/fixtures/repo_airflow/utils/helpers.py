def default_args(owner: str, depends_on_past: bool = True):
    """Kein Operator, kein Signal: gibt nur ein Dict zurueck."""
    return {"owner": owner, "depends_on_past": depends_on_past}
