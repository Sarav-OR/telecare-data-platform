{{ vault_sat(source='stg_crm__insurance_plans', parent_hash_key='hk_insurance_plan', hashdiff_column='hd_insurance_plan',
             payload=['plan_code', 'insurer_id', 'plan_name', 'plan_model', 'telmed_first_contact_required', 'premium_discount_pct']) }}
