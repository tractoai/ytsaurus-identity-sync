# ytsaurus-identity-sync

## Installing
To install application to your k8s cluster — use helm chart with something like that:
```
helm upgrade --install --wait \
 -f ytsaurus-identity-sync.values.yaml \
 -n idsync --create-namespace \
 idsync oci://ghcr.io/tractoai/ytsaurus-identity-sync-chart \
--version 0.2.0
```
Examples for helm values can be found in the [examples](examples) directory.  
All configuration options for app can be found in [main/config.go](main/config.go) file.


## Official release
To issue an official release of app — create new release at the [releases](https://github.com/tractoai/ytsaurus-identity-sync/releases) tab with some release notes.  
For the app release create a tag matching pattern `release/X.X.X`.  
For the chart release create a tag matching pattern `release-helm/X.X.X`.
Images will be build automatically on release tag creation.

## Development releases
Application docker image and helm oci images are created on each commit to the main branch and uploaded to Github Packages.  
[app registry](https://github.com/tractoai/ytsaurus-identity-sync/pkgs/container/ytsaurus-identity-sync)  
[chart registry](https://github.com/tractoai/ytsaurus-identity-sync/pkgs/container/ytsaurus-identity-sync-chart)

N.B. helm chart versions from the [helm docs](https://helm.sh/docs/topics/registries/#oci-feature-deprecation-and-behavior-changes-with-v380)
> OCI artifact references (e.g. tags) do not support the plus sign (+). To support
storing semantic versions, Helm adopts the convention of changing plus (+) to
an underscore (_) in chart version tags when pushing to a registry and back to
a plus (+) when pulling from a registry.

It means that Helm requires semver for chart versions and we use `0.0.0+<date>-<commit-sha>`, but `+` is replaced by helm on upload/download to `_`, 
therefore  
`ghcr.io/tractoai/ytsaurus-identity-sync-chart:0.0.0_2024-06-11-c9f077f`   
must be pulled as   
`helm pull oci://ghcr.io/tractoai/ytsaurus-identity-sync-chart --version=0.0.0+2024-06-11-c9f077f`
(note the `+`sign).

