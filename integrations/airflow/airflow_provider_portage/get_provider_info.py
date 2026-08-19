"""Standard Airflow provider metadata entrypoint (referenced by
pyproject.toml's [project.entry-points.apache_airflow_provider] block) --
lets `airflow providers list` and the Connections UI discover this
package's connection type and components."""


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-portage",
        "name": "Portage",
        "description": "Submit and poll Portage (portable Spark workload) runs from Airflow.",
        "connection-types": [
            {
                "connection-type": "portage",
                "hook-class-name": "airflow_provider_portage.hooks.portage.PortageHook",
            }
        ],
        "versions": ["0.1.0"],
    }
