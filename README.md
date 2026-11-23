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
All configuration options for app can be found in [internal/config/config.go](internal/config/config.go) file.

## Azure user ID rebinding

When an Azure user is recreated with the same login and a new ID, the default behavior
is to ban/remove the old YTsaurus user and create a new one. Creation fails while the
old user still occupies the name during `app.ban_before_remove_duration`.

To reuse the existing managed YTsaurus user, enable this option in the application
configuration (inside `syncConfig` when using Helm):

```yaml
azure:
  allow_user_id_rebinding: true
```

This option defaults to `false` and only applies to Azure users. Matching uses the
YTsaurus username after `app.username_replacements` and lowercasing. The previous
Azure ID must be absent from the current filtered user list, and the new ID must
not already be linked to a YTsaurus user. Ambiguous usernames or IDs stop user
synchronization before any mutations. Manually managed users are not rebound.

Rebinding updates the source attributes and clears the ban through the normal user
update operation. It preserves the YTsaurus object ID and existing permissions;
group memberships are then reconciled with Azure using the new ID. Enable it only
when a recreated Azure identity should retain the existing user's access.
It honors `ytsaurus.apply_user_changes` and is counted as a user update in metrics.


## Official release
To issue an official release of app — create new release at the [releases](https://github.com/tractoai/ytsaurus-identity-sync/releases) tab with some release notes.  
For the app release create a tag matching pattern `release/X.X.X`.  
For the chart release create a tag matching pattern `release-helm/X.X.X`.
Images will be build automatically on release tag creation.

## Development releases
Application docker image and helm oci images are created on each commit to the main branch and uploaded to Github Packages.  
[app registry](https://github.com/tractoai/ytsaurus-identity-sync/pkgs/container/ytsaurus-identity-sync)  
[chart registry](https://github.com/tractoai/ytsaurus-identity-sync/pkgs/container/ytsaurus-identity-sync-chart)
