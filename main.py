from typing import cast, Literal

import typer
from deweypy.context import set_entrypoint
from deweypy.auth import (
    resolve_api_key,
    set_api_key,
)
from deweypy.context import main_context
from rich import print as rprint

app = typer.Typer()


_shared_api_key_option = typer.Option(
    None, "--api-key", help="Your Dewey API Key.", show_default=False
)
_shared_print_debug_info_option = typer.Option(
    False, "--print-debug-info", help="Print debug info?", show_default=False
)


def _handle_api_key_option(
    api_key: str | None = typer.Option(
        None,
        "--api-key",
        prompt="Paste your API key",
        hide_input=True,
        help="Your Dewey API Key.",
        show_default=False,
    ),
):
    def prompt_callback() -> str:
        return cast(
            str,
            typer.prompt(
                "Paste your API key (input hidden, won't trigger until you paste/type and then press Enter)",
                hide_input=True,
            ),
        )

    resolved_api_key, resolved_source = resolve_api_key(
        potentially_provided_value=api_key,
        callback_if_missing=prompt_callback,
        invalid_exception_class=RuntimeError,
    )
    assert resolved_source in ("provided", "environment", "callback"), "Post-condition"
    api_key_source: Literal["cli_args", "cli_fallback", "environment"]
    if resolved_source == "provided":
        api_key_source = "cli_args"
    elif resolved_source == "environment":
        api_key_source = "environment"
    else:
        assert resolved_source == "callback", "Pre-condition"
        api_key_source = "cli_fallback"
    set_api_key(resolved_api_key, api_key_source=api_key_source)
    set_entrypoint("cli")

    return resolved_api_key


@app.callback()
def main(
    *,
    api_key: str = _shared_api_key_option,
    print_debug_info: bool = _shared_print_debug_info_option,
):
    set_entrypoint("cli")

    _handle_api_key_option(api_key)

    if print_debug_info:
        rprint("--- Initial Debug Info ---")
        prefix = "Main Global Callback Main Context:"
        rprint(f"{prefix} context={main_context}")
        rprint(f"{prefix} entrypoint={main_context.entrypoint}")
        rprint(f"{prefix} _api_key={main_context.api_key}")
        rprint(f"{prefix} api_key_source={main_context.api_key_source}")
        rprint(f"{prefix} api_key_repr_preview={main_context.api_key_repr_preview}")
        rprint("---            ---")


@app.command()
def download(dataset: str = typer.Argument(..., help="The dataset to download.")):
    pass


if __name__ == "__main__":
    typer.run(main)
