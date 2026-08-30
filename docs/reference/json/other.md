<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# Other shared attributes

Attributes that belong to no single flow kind — they appear nested inside a block (an `encrypted_columns[]` entry, for example) rather than at the top level of a flow.


!!! info "1 attributes"
    Every attribute below is also available in the Spec Builder's attribute
    inspector — click the **i** beside any field to see this same content
    without leaving the form.


## Summary

| Attribute | Type | Required | Default |
|---|---|---|---|
| [`source_data_type`](#source-data-type) | string | no | — |

## Attributes

### `source_data_type` { #source-data-type }

Declares the original Spark type of an encrypted column.


Encryption replaces a column's physical type with ciphertext binary, so the pre-encryption type is recorded as the Unity Catalog original_data_type tag -- which is what a downstream source_inputs[].decrypted_columns[].cast_to_type is validated against. Optional and safely defaulted: omit it and the framework uses the type Spark reports at encryption time (the pre-v1.4.0 behaviour). Declare it and a source column that quietly changes type fails loudly at encryption time, naming declared and observed, instead of silently re-tagging and breaking the decrypt side later.


**Type** `string` · **Required** no


---
