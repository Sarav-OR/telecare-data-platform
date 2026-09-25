-- Multi-source: a referral can point to a partner before the weekly directory snapshot lists it.
{{ vault_hub(sources=['stg_crm__partners', 'stg_ehr__referral'], hash_key='hk_partner', business_keys=['partner_id']) }}
