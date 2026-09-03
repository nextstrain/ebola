# CHANGELOG

We use this CHANGELOG to document breaking changes, new features, bug fixes,
and config value changes that may affect both the usage of the workflows and
the outputs of the workflows. See the [changelog for the ncov
repository](https://github.com/nextstrain/ncov/blob/HEAD/docs/src/reference/change_log.md)
for an example of formatting.

## 2026

* 03 September 2026: The config param `strain_id_field` has been replaced by
  input-specific `id_field` params. Restore previous default behavior by setting
  `id_field: accession` on each of your inputs. This change requires a minimum
  Augur version of 34.0.0.
