from swgoh.bot.commands.misoperaciones import _format_operations_body


def test_misoperaciones_html_format():
    body = (
        "Tatooine & Jardines\n"
        "- IG-88 (Operación <especial>)\n"
        "- Luke (Ataque)\n"
    )
    formatted = _format_operations_body(body)

    assert "<b>Tatooine &amp; Jardines</b>" in formatted
    assert "- IG-88 (Operación &lt;especial&gt;)" in formatted
