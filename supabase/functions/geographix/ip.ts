import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  const D = "|&|";
  const N = "purrNULL";

  filter = filter ? filter : "";

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT * FROM (
    WITH w AS (
      SELECT
        uwi                        AS w_uwi
      FROM well
    ),
    t AS (
      SELECT
        uwi                        AS id_t_uwi,
        source                     AS id_t_source,
        test_type                  AS id_t_test_type,
        run_number                 AS id_t_run_number,
        MAX(row_changed_date)      AS max_row_changed_date,
        LIST(IFNULL(base_depth,                 '${N}', CAST(base_depth AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_base_depth,
        LIST(IFNULL(base_form,                  '${N}', CAST(base_form AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_base_form,
        LIST(IFNULL(bottom_choke_desc,          '${N}', CAST(bottom_choke_desc AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_bottom_choke_desc,
        LIST(IFNULL(casing_press,               '${N}', CAST(casing_press AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_casing_press,
        LIST(IFNULL(choke_size_desc,            '${N}', CAST(choke_size_desc AS VARCHAR)),           '${D}' ORDER BY test_number)  AS t_choke_size_desc,
        LIST(IFNULL(flow_press,                 '${N}', CAST(flow_press AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_flow_press,
        LIST(IFNULL(flow_temp,                  '${N}', CAST(flow_temp AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_flow_temp,
        LIST(IFNULL(gas_flow_amt,               '${N}', CAST(gas_flow_amt AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_gas_flow_amt,
        LIST(IFNULL(gas_flow_amt_uom,           '${N}', CAST(gas_flow_amt_uom AS VARCHAR)),          '${D}' ORDER BY test_number)  AS t_gas_flow_amt_uom,
        LIST(IFNULL(gor,                        '${N}', CAST(gor AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS t_gor,
        LIST(IFNULL(gx_base_form_alias,         '${N}', CAST(gx_base_form_alias AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_gx_base_form_alias,
        LIST(IFNULL(gx_bhp_z,                   '${N}', CAST(gx_bhp_z AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS t_gx_bhp_z,
        LIST(IFNULL(gx_co2_pct,                 '${N}', CAST(gx_co2_pct AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_gx_co2_pct,
        LIST(IFNULL(gx_conversion_factor,       '${N}', CAST(gx_conversion_factor AS VARCHAR)),      '${D}' ORDER BY test_number)  AS t_gx_conversion_factor,
        LIST(IFNULL(gx_cushion_type,            '${N}', CAST(gx_cushion_type AS VARCHAR)),           '${D}' ORDER BY test_number)  AS t_gx_cushion_type,
        LIST(IFNULL(gx_cushion_vol,             '${N}', CAST(gx_cushion_vol AS VARCHAR)),            '${D}' ORDER BY test_number)  AS t_gx_cushion_vol,
        LIST(IFNULL(gx_cushion_vol_ouom,        '${N}', CAST(gx_cushion_vol_ouom AS VARCHAR)),       '${D}' ORDER BY test_number)  AS t_gx_cushion_vol_ouom,
        LIST(IFNULL(gx_ft_flow_amt,             '${N}', CAST(gx_ft_flow_amt AS VARCHAR)),            '${D}' ORDER BY test_number)  AS t_gx_ft_flow_amt,
        LIST(IFNULL(gx_ft_flow_amt_uom,         '${N}', CAST(gx_ft_flow_amt_uom AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_gx_ft_flow_amt_uom,
        LIST(IFNULL(gx_gas_cum,                 '${N}', CAST(gx_gas_cum AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_gx_gas_cum,
        LIST(IFNULL(gx_max_ft_flow_rate,        '${N}', CAST(gx_max_ft_flow_rate AS VARCHAR)),       '${D}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate,
        LIST(IFNULL(gx_max_ft_flow_rate_uom,    '${N}', CAST(gx_max_ft_flow_rate_uom AS VARCHAR)),   '${D}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate_uom,
        LIST(IFNULL(gx_recorder_depth,          '${N}', CAST(gx_recorder_depth AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_gx_recorder_depth,
        LIST(IFNULL(gx_top_form_alias,          '${N}', CAST(gx_top_form_alias AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_gx_top_form_alias,
        LIST(IFNULL(gx_tracer,                  '${N}', CAST(gx_tracer AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_gx_tracer,
        LIST(IFNULL(gx_z_factor,                '${N}', CAST(gx_z_factor AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_gx_z_factor,
        LIST(IFNULL(h2s_pct,                    '${N}', CAST(h2s_pct AS VARCHAR)),                   '${D}' ORDER BY test_number)  AS t_h2s_pct,
        LIST(IFNULL(max_gas_flow_rate,          '${N}', CAST(max_gas_flow_rate AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_max_gas_flow_rate,
        LIST(IFNULL(max_hydrostatic_press,      '${N}', CAST(max_hydrostatic_press AS VARCHAR)),     '${D}' ORDER BY test_number)  AS t_max_hydrostatic_press,
        LIST(IFNULL(max_oil_flow_rate,          '${N}', CAST(max_oil_flow_rate AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_max_oil_flow_rate,
        LIST(IFNULL(max_water_flow_rate,        '${N}', CAST(max_water_flow_rate AS VARCHAR)),       '${D}' ORDER BY test_number)  AS t_max_water_flow_rate,
        LIST(IFNULL(oil_flow_amt,               '${N}', CAST(oil_flow_amt AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_oil_flow_amt,
        LIST(IFNULL(oil_flow_amt_uom,           '${N}', CAST(oil_flow_amt_uom AS VARCHAR)),          '${D}' ORDER BY test_number)  AS t_oil_flow_amt_uom,
        LIST(IFNULL(oil_gravity,                '${N}', CAST(oil_gravity AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_oil_gravity,
        LIST(IFNULL(primary_fluid_recovered,    '${N}', CAST(primary_fluid_recovered AS VARCHAR)),   '${D}' ORDER BY test_number)  AS t_primary_fluid_recovered,
        LIST(IFNULL(production_method,          '${N}', CAST(production_method AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_production_method,
        LIST(IFNULL(remark,                     '${N}', CAST(remark AS VARCHAR)),                    '${D}' ORDER BY test_number)  AS t_remark,
        LIST(IFNULL(report_temp,                '${N}', CAST(report_temp AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_report_temp,
        LIST(IFNULL(row_changed_date,           '${N}', CAST(row_changed_date AS VARCHAR)),          '${D}' ORDER BY test_number)  AS t_row_changed_date,
        LIST(IFNULL(run_number,                 '${N}', CAST(run_number AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_run_number,
        LIST(IFNULL(shut_off_type,              '${N}', CAST(shut_off_type AS VARCHAR)),             '${D}' ORDER BY test_number)  AS t_shut_off_type,
        LIST(IFNULL(source,                     '${N}', CAST(source AS VARCHAR)),                    '${D}' ORDER BY test_number)  AS t_source,
        LIST(IFNULL(test_date,                  '${N}', CAST(test_date AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_test_date,
        LIST(IFNULL(test_duration,              '${N}', CAST(test_duration AS VARCHAR)),             '${D}' ORDER BY test_number)  AS t_test_duration,
        LIST(IFNULL(test_number,                '${N}', CAST(test_number AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_test_number,
        LIST(IFNULL(test_subtype,               '${N}', CAST(test_subtype AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_test_subtype,
        LIST(IFNULL(test_type,                  '${N}', CAST(test_type AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_test_type,
        LIST(IFNULL(top_choke_desc,             '${N}', CAST(top_choke_desc AS VARCHAR)),            '${D}' ORDER BY test_number)  AS t_top_choke_desc,
        LIST(IFNULL(top_depth,                  '${N}', CAST(top_depth AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_top_depth,
        LIST(IFNULL(top_form,                   '${N}', CAST(top_form AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS t_top_form,
        LIST(IFNULL(uwi,                        '${N}', CAST(uwi AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS t_uwi,
        LIST(IFNULL(water_flow_amt,             '${N}', CAST(water_flow_amt AS VARCHAR)),            '${D}' ORDER BY test_number)  AS t_water_flow_amt,
        LIST(IFNULL(water_flow_amt_uom,         '${N}', CAST(water_flow_amt_uom AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_water_flow_amt_uom
      FROM well_test
      WHERE test_type = 'IP'
      GROUP BY uwi, source, test_type, run_number
    )
    SELECT
      w.*,
      t.*
    FROM w
    JOIN t ON w.w_uwi = t.id_t_uwi
    ) x`;

  // 203-10-06 | memory requirements are just too high. Removed well_treatments.
  // let select = `SELECT * FROM (
  //   WITH w AS (
  //     SELECT
  //       uwi                        AS w_uwi
  //     FROM well
  //   ),
  //   t AS (
  //     SELECT
  //       uwi                        AS id_t_uwi,
  //       source                     AS id_t_source,
  //       test_type                  AS id_t_test_type,
  //       run_number                 AS id_t_run_number,
  //       LIST(IFNULL(base_depth,                 '${N}', CAST(base_depth AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_base_depth,
  //       LIST(IFNULL(base_form,                  '${N}', CAST(base_form AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_base_form,
  //       LIST(IFNULL(bottom_choke_desc,          '${N}', CAST(bottom_choke_desc AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_bottom_choke_desc,
  //       LIST(IFNULL(casing_press,               '${N}', CAST(casing_press AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_casing_press,
  //       LIST(IFNULL(choke_size_desc,            '${N}', CAST(choke_size_desc AS VARCHAR)),           '${D}' ORDER BY test_number)  AS t_choke_size_desc,
  //       LIST(IFNULL(flow_press,                 '${N}', CAST(flow_press AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_flow_press,
  //       LIST(IFNULL(flow_temp,                  '${N}', CAST(flow_temp AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_flow_temp,
  //       LIST(IFNULL(gas_flow_amt,               '${N}', CAST(gas_flow_amt AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_gas_flow_amt,
  //       LIST(IFNULL(gas_flow_amt_uom,           '${N}', CAST(gas_flow_amt_uom AS VARCHAR)),          '${D}' ORDER BY test_number)  AS t_gas_flow_amt_uom,
  //       LIST(IFNULL(gor,                        '${N}', CAST(gor AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS t_gor,
  //       LIST(IFNULL(gx_base_form_alias,         '${N}', CAST(gx_base_form_alias AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_gx_base_form_alias,
  //       LIST(IFNULL(gx_bhp_z,                   '${N}', CAST(gx_bhp_z AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS t_gx_bhp_z,
  //       LIST(IFNULL(gx_co2_pct,                 '${N}', CAST(gx_co2_pct AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_gx_co2_pct,
  //       LIST(IFNULL(gx_conversion_factor,       '${N}', CAST(gx_conversion_factor AS VARCHAR)),      '${D}' ORDER BY test_number)  AS t_gx_conversion_factor,
  //       LIST(IFNULL(gx_cushion_type,            '${N}', CAST(gx_cushion_type AS VARCHAR)),           '${D}' ORDER BY test_number)  AS t_gx_cushion_type,
  //       LIST(IFNULL(gx_cushion_vol,             '${N}', CAST(gx_cushion_vol AS VARCHAR)),            '${D}' ORDER BY test_number)  AS t_gx_cushion_vol,
  //       LIST(IFNULL(gx_cushion_vol_ouom,        '${N}', CAST(gx_cushion_vol_ouom AS VARCHAR)),       '${D}' ORDER BY test_number)  AS t_gx_cushion_vol_ouom,
  //       LIST(IFNULL(gx_ft_flow_amt,             '${N}', CAST(gx_ft_flow_amt AS VARCHAR)),            '${D}' ORDER BY test_number)  AS t_gx_ft_flow_amt,
  //       LIST(IFNULL(gx_ft_flow_amt_uom,         '${N}', CAST(gx_ft_flow_amt_uom AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_gx_ft_flow_amt_uom,
  //       LIST(IFNULL(gx_gas_cum,                 '${N}', CAST(gx_gas_cum AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_gx_gas_cum,
  //       LIST(IFNULL(gx_max_ft_flow_rate,        '${N}', CAST(gx_max_ft_flow_rate AS VARCHAR)),       '${D}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate,
  //       LIST(IFNULL(gx_max_ft_flow_rate_uom,    '${N}', CAST(gx_max_ft_flow_rate_uom AS VARCHAR)),   '${D}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate_uom,
  //       LIST(IFNULL(gx_recorder_depth,          '${N}', CAST(gx_recorder_depth AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_gx_recorder_depth,
  //       LIST(IFNULL(gx_top_form_alias,          '${N}', CAST(gx_top_form_alias AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_gx_top_form_alias,
  //       LIST(IFNULL(gx_tracer,                  '${N}', CAST(gx_tracer AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_gx_tracer,
  //       LIST(IFNULL(gx_z_factor,                '${N}', CAST(gx_z_factor AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_gx_z_factor,
  //       LIST(IFNULL(h2s_pct,                    '${N}', CAST(h2s_pct AS VARCHAR)),                   '${D}' ORDER BY test_number)  AS t_h2s_pct,
  //       LIST(IFNULL(max_gas_flow_rate,          '${N}', CAST(max_gas_flow_rate AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_max_gas_flow_rate,
  //       LIST(IFNULL(max_hydrostatic_press,      '${N}', CAST(max_hydrostatic_press AS VARCHAR)),     '${D}' ORDER BY test_number)  AS t_max_hydrostatic_press,
  //       LIST(IFNULL(max_oil_flow_rate,          '${N}', CAST(max_oil_flow_rate AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_max_oil_flow_rate,
  //       LIST(IFNULL(max_water_flow_rate,        '${N}', CAST(max_water_flow_rate AS VARCHAR)),       '${D}' ORDER BY test_number)  AS t_max_water_flow_rate,
  //       LIST(IFNULL(oil_flow_amt,               '${N}', CAST(oil_flow_amt AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_oil_flow_amt,
  //       LIST(IFNULL(oil_flow_amt_uom,           '${N}', CAST(oil_flow_amt_uom AS VARCHAR)),          '${D}' ORDER BY test_number)  AS t_oil_flow_amt_uom,
  //       LIST(IFNULL(oil_gravity,                '${N}', CAST(oil_gravity AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_oil_gravity,
  //       LIST(IFNULL(primary_fluid_recovered,    '${N}', CAST(primary_fluid_recovered AS VARCHAR)),   '${D}' ORDER BY test_number)  AS t_primary_fluid_recovered,
  //       LIST(IFNULL(production_method,          '${N}', CAST(production_method AS VARCHAR)),         '${D}' ORDER BY test_number)  AS t_production_method,
  //       LIST(IFNULL(remark,                     '${N}', CAST(remark AS VARCHAR)),                    '${D}' ORDER BY test_number)  AS t_remark,
  //       LIST(IFNULL(report_temp,                '${N}', CAST(report_temp AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_report_temp,
  //       LIST(IFNULL(row_changed_date,           '${N}', CAST(row_changed_date AS VARCHAR)),          '${D}' ORDER BY test_number)  AS t_row_changed_date,
  //       LIST(IFNULL(run_number,                 '${N}', CAST(run_number AS VARCHAR)),                '${D}' ORDER BY test_number)  AS t_run_number,
  //       LIST(IFNULL(shut_off_type,              '${N}', CAST(shut_off_type AS VARCHAR)),             '${D}' ORDER BY test_number)  AS t_shut_off_type,
  //       LIST(IFNULL(source,                     '${N}', CAST(source AS VARCHAR)),                    '${D}' ORDER BY test_number)  AS t_source,
  //       LIST(IFNULL(test_date,                  '${N}', CAST(test_date AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_test_date,
  //       LIST(IFNULL(test_duration,              '${N}', CAST(test_duration AS VARCHAR)),             '${D}' ORDER BY test_number)  AS t_test_duration,
  //       LIST(IFNULL(test_number,                '${N}', CAST(test_number AS VARCHAR)),               '${D}' ORDER BY test_number)  AS t_test_number,
  //       LIST(IFNULL(test_subtype,               '${N}', CAST(test_subtype AS VARCHAR)),              '${D}' ORDER BY test_number)  AS t_test_subtype,
  //       LIST(IFNULL(test_type,                  '${N}', CAST(test_type AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_test_type,
  //       LIST(IFNULL(top_choke_desc,             '${N}', CAST(top_choke_desc AS VARCHAR)),            '${D}' ORDER BY test_number)  AS t_top_choke_desc,
  //       LIST(IFNULL(top_depth,                  '${N}', CAST(top_depth AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS t_top_depth,
  //       LIST(IFNULL(top_form,                   '${N}', CAST(top_form AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS t_top_form,
  //       LIST(IFNULL(uwi,                        '${N}', CAST(uwi AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS t_uwi,
  //       LIST(IFNULL(water_flow_amt,             '${N}', CAST(water_flow_amt AS VARCHAR)),            '${D}' ORDER BY test_number)  AS t_water_flow_amt,
  //       LIST(IFNULL(water_flow_amt_uom,         '${N}', CAST(water_flow_amt_uom AS VARCHAR)),        '${D}' ORDER BY test_number)  AS t_water_flow_amt_uom
  //     FROM well_test
  //     WHERE test_type = 'IP'
  //     GROUP BY uwi, source, test_type, run_number
  //   ),
  //   r AS (
  //     SELECT
  //       uwi                        AS id_r_uwi,
  //       source                     AS id_r_source,
  //       test_number                AS id_r_test_number,
  //       run_number                 AS id_r_run_number,
  //       test_type                  AS id_r_test_type,
  //       LIST(IFNULL(additive_type,              '${N}', CAST(additive_type AS VARCHAR)),             '${D}' ORDER BY test_number)  AS r_additive_type,
  //       LIST(IFNULL(base_depth,                 '${N}', CAST(base_depth AS VARCHAR)),                '${D}' ORDER BY test_number)  AS r_base_depth,
  //       LIST(IFNULL(base_form,                  '${N}', CAST(base_form AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS r_base_form,
  //       LIST(IFNULL(completion_obs_no,          '${N}', CAST(completion_obs_no AS VARCHAR)),         '${D}' ORDER BY test_number)  AS r_completion_obs_no,
  //       LIST(IFNULL(completion_source,          '${N}', CAST(completion_source AS VARCHAR)),         '${D}' ORDER BY test_number)  AS r_completion_source,
  //       LIST(IFNULL(form_break_press,           '${N}', CAST(form_break_press AS VARCHAR)),          '${D}' ORDER BY test_number)  AS r_form_break_press,
  //       LIST(IFNULL(gx_base_form_alias,         '${N}', CAST(gx_base_form_alias AS VARCHAR)),        '${D}' ORDER BY test_number)  AS r_gx_base_form_alias,
  //       LIST(IFNULL(gx_case_name,               '${N}', CAST(gx_case_name AS VARCHAR)),              '${D}' ORDER BY test_number)  AS r_gx_case_name,
  //       LIST(IFNULL(gx_frac_grad_act,           '${N}', CAST(gx_frac_grad_act AS VARCHAR)),          '${D}' ORDER BY test_number)  AS r_gx_frac_grad_act,
  //       LIST(IFNULL(gx_frac_grad_des,           '${N}', CAST(gx_frac_grad_des AS VARCHAR)),          '${D}' ORDER BY test_number)  AS r_gx_frac_grad_des,
  //       LIST(IFNULL(gx_frac_height_act,         '${N}', CAST(gx_frac_height_act AS VARCHAR)),        '${D}' ORDER BY test_number)  AS r_gx_frac_height_act,
  //       LIST(IFNULL(gx_frac_height_des,         '${N}', CAST(gx_frac_height_des AS VARCHAR)),        '${D}' ORDER BY test_number)  AS r_gx_frac_height_des,
  //       LIST(IFNULL(gx_frac_length_act,         '${N}', CAST(gx_frac_length_act AS VARCHAR)),        '${D}' ORDER BY test_number)  AS r_gx_frac_length_act,
  //       LIST(IFNULL(gx_frac_length_des,         '${N}', CAST(gx_frac_length_des AS VARCHAR)),        '${D}' ORDER BY test_number)  AS r_gx_frac_length_des,
  //       LIST(IFNULL(gx_frac_perm_act,           '${N}', CAST(gx_frac_perm_act AS VARCHAR)),          '${D}' ORDER BY test_number)  AS r_gx_frac_perm_act,
  //       LIST(IFNULL(gx_frac_perm_des,           '${N}', CAST(gx_frac_perm_des AS VARCHAR)),          '${D}' ORDER BY test_number)  AS r_gx_frac_perm_des,
  //       LIST(IFNULL(gx_frac_width_act,          '${N}', CAST(gx_frac_width_act AS VARCHAR)),         '${D}' ORDER BY test_number)  AS r_gx_frac_width_act,
  //       LIST(IFNULL(gx_frac_width_des,          '${N}', CAST(gx_frac_width_des AS VARCHAR)),         '${D}' ORDER BY test_number)  AS r_gx_frac_width_des,
  //       LIST(IFNULL(gx_instant_si_press_time1,  '${N}', CAST(gx_instant_si_press_time1 AS VARCHAR)), '${D}' ORDER BY test_number)  AS r_gx_instant_si_press_time1,
  //       LIST(IFNULL(gx_instant_si_press_time2,  '${N}', CAST(gx_instant_si_press_time2 AS VARCHAR)), '${D}' ORDER BY test_number)  AS r_gx_instant_si_press_time2,
  //       LIST(IFNULL(gx_instant_si_press_time3,  '${N}', CAST(gx_instant_si_press_time3 AS VARCHAR)), '${D}' ORDER BY test_number)  AS r_gx_instant_si_press_time3,
  //       LIST(IFNULL(gx_instant_si_press_time4,  '${N}', CAST(gx_instant_si_press_time4 AS VARCHAR)), '${D}' ORDER BY test_number)  AS r_gx_instant_si_press_time4,
  //       LIST(IFNULL(gx_interval_id,             '${N}', CAST(gx_interval_id AS VARCHAR)),            '${D}' ORDER BY test_number)  AS r_gx_interval_id,
  //       LIST(IFNULL(gx_time1,                   '${N}', CAST(gx_time1 AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS r_gx_time1,
  //       LIST(IFNULL(gx_time2,                   '${N}', CAST(gx_time2 AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS r_gx_time2,
  //       LIST(IFNULL(gx_time3,                   '${N}', CAST(gx_time3 AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS r_gx_time3,
  //       LIST(IFNULL(gx_time4,                   '${N}', CAST(gx_time4 AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS r_gx_time4,
  //       LIST(IFNULL(gx_top_form_alias,          '${N}', CAST(gx_top_form_alias AS VARCHAR)),         '${D}' ORDER BY test_number)  AS r_gx_top_form_alias,
  //       LIST(IFNULL(gx_zone_id,                 '${N}', CAST(gx_zone_id AS VARCHAR)),                '${D}' ORDER BY test_number)  AS r_gx_zone_id,
  //       LIST(IFNULL(injection_rate,             '${N}', CAST(injection_rate AS VARCHAR)),            '${D}' ORDER BY test_number)  AS r_injection_rate,
  //       LIST(IFNULL(instant_si_press,           '${N}', CAST(instant_si_press AS VARCHAR)),          '${D}' ORDER BY test_number)  AS r_instant_si_press,
  //       LIST(IFNULL(proppant_agent_amt,         '${N}', CAST(proppant_agent_amt AS VARCHAR)),        '${D}' ORDER BY test_number)  AS r_proppant_agent_amt,
  //       LIST(IFNULL(proppant_agent_type,        '${N}', CAST(proppant_agent_type AS VARCHAR)),       '${D}' ORDER BY test_number)  AS r_proppant_agent_type,
  //       LIST(IFNULL(proppant_amt_uom,           '${N}', CAST(proppant_amt_uom AS VARCHAR)),          '${D}' ORDER BY test_number)  AS r_proppant_amt_uom,
  //       LIST(IFNULL(remark,                     '${N}', CAST(remark AS VARCHAR)),                    '${D}' ORDER BY test_number)  AS r_remark,
  //       LIST(IFNULL(row_changed_date,           '${N}', CAST(row_changed_date AS VARCHAR)),          '${D}' ORDER BY test_number)  AS r_row_changed_date,
  //       LIST(IFNULL(run_number,                 '${N}', CAST(run_number AS VARCHAR)),                '${D}' ORDER BY test_number)  AS r_run_number,
  //       LIST(IFNULL(source,                     '${N}', CAST(source AS VARCHAR)),                    '${D}' ORDER BY test_number)  AS r_source,
  //       LIST(IFNULL(stage_no,                   '${N}', CAST(stage_no AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS r_stage_no,
  //       LIST(IFNULL(test_number,                '${N}', CAST(test_number AS VARCHAR)),               '${D}' ORDER BY test_number)  AS r_test_number,
  //       LIST(IFNULL(test_type,                  '${N}', CAST(test_type AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS r_test_type,
  //       LIST(IFNULL(top_depth,                  '${N}', CAST(top_depth AS VARCHAR)),                 '${D}' ORDER BY test_number)  AS r_top_depth,
  //       LIST(IFNULL(top_form,                   '${N}', CAST(top_form AS VARCHAR)),                  '${D}' ORDER BY test_number)  AS r_top_form,
  //       LIST(IFNULL(treatment_amt,              '${N}', CAST(treatment_amt AS VARCHAR)),             '${D}' ORDER BY test_number)  AS r_treatment_amt,
  //       LIST(IFNULL(treatment_amt_uom,          '${N}', CAST(treatment_amt_uom AS VARCHAR)),         '${D}' ORDER BY test_number)  AS r_treatment_amt_uom,
  //       LIST(IFNULL(treatment_date,             '${N}', CAST(treatment_date AS VARCHAR)),            '${D}' ORDER BY test_number)  AS r_treatment_date,
  //       LIST(IFNULL(treatment_fluid_type,       '${N}', CAST(treatment_fluid_type AS VARCHAR)),      '${D}' ORDER BY test_number)  AS r_treatment_fluid_type,
  //       LIST(IFNULL(treatment_obs_no,           '${N}', CAST(treatment_obs_no AS VARCHAR)),          '${D}' ORDER BY test_number)  AS r_treatment_obs_no,
  //       LIST(IFNULL(treatment_press,            '${N}', CAST(treatment_press AS VARCHAR)),           '${D}' ORDER BY test_number)  AS r_treatment_press,
  //       LIST(IFNULL(treatment_status,           '${N}', CAST(treatment_status AS VARCHAR)),          '${D}' ORDER BY test_number)  AS r_treatment_status,
  //       LIST(IFNULL(treatment_type,             '${N}', CAST(treatment_type AS VARCHAR)),            '${D}' ORDER BY test_number)  AS r_treatment_type,
  //       LIST(IFNULL(uwi,                        '${N}', CAST(uwi AS VARCHAR)),                       '${D}' ORDER BY test_number)  AS r_uwi,
  //       LIST(IFNULL(well_test_source,           '${N}', CAST(well_test_source AS VARCHAR)),          '${D}' ORDER BY test_number)  AS r_well_test_source
  //     FROM well_treatment
  //     GROUP BY uwi, source, test_number, run_number, test_type
  //   )
  //   SELECT
  //     w.*,
  //     t.*,
  //     r.*
  //   FROM w
  //   JOIN t ON w.w_uwi = t.id_t_uwi
  //   JOIN r ON w.w_uwi = r.id_r_uwi
  //   ) x`;

  const order = `ORDER BY w_uwi, id_t_source, id_t_test_type, id_t_run_number`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  return {
    select: select,
    count: count,
    order: order,
    where: where,
  };
};

const xformer = (args) => {
  const D = "|&|";
  const N = "purrNULL";

  let { func, key, typ, arg, obj } = args;

  const ensureType = (type: string, val: any) => {
    if (val == null) {
      return null;
    } else if (type === "object") {
      console.log("UNEXPECTED OBJECT TYPE! (needs xformer)", type);
      console.log(val);
      return null;
    } else if (type === "string") {
      return val.replace(/[\u0000-\u001F\u007F-\u009F]/g, "");
    } else if (type === "number") {
      if (val.toString().replace(/\s/g, "") === "") {
        return null;
      }
      let n = Number(val);
      return isNaN(n) ? null : n;
    } else if (type === "date") {
      try {
        return new Date(val).toISOString();
      } catch (error) {
        return null;
      }
    } else {
      console.log("ENSURE TYPE SOMETHING ELSE (xformer)", type);
      return "XFORM ME";
    }
  };

  if (obj[key] == null) {
    return null;
  }

  switch (func) {
    case "delimited_array_with_nulls":
      return (() => {
        try {
          return obj[key]
            .split(D)
            .map((v) => (v === N ? null : ensureType(typ, v)));
        } catch (error) {
          console.log("ERROR", error);
          return;
        }
      })();
    default:
      return ensureType(typ, obj[key]);
  }
};

const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },

  // WELL_TEST

  id_t_uwi: {
    ts_type: "string",
  },
  id_t_source: {
    ts_type: "string",
  },
  id_t_run_number: {
    ts_type: "string",
  },
  id_t_test_type: {
    ts_type: "string",
  },
  max_row_changed_date: {
    ts_type: "date",
  },
  t_base_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_base_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_bottom_choke_desc: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_casing_press: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_choke_size_desc: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_flow_press: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_flow_temp: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gas_flow_amt: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gas_flow_amt_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gor: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_base_form_alias: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_bhp_z: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_co2_pct: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_conversion_factor: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_cushion_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_cushion_vol: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_cushion_vol_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_ft_flow_amt: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_ft_flow_amt_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_gas_cum: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_max_ft_flow_rate: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_max_ft_flow_rate_uom: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_recorder_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_gx_top_form_alias: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_tracer: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_gx_z_factor: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_h2s_pct: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_max_gas_flow_rate: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_max_hydrostatic_press: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_max_oil_flow_rate: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_max_water_flow_rate: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_oil_flow_amt: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_oil_flow_amt_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_oil_gravity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_primary_fluid_recovered: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_production_method: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_report_temp: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  t_run_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_shut_off_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_test_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  t_test_duration: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_test_number: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_test_subtype: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_test_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_top_choke_desc: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_top_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_top_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  t_water_flow_amt: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  t_water_flow_amt_uom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },

  // // WELL_TREATMENT

  // id_r_uwi: {
  //   ts_type: "string",
  // },
  // id_r_source: {
  //   ts_type: "string",
  // },
  // id_r_test_number: {
  //   ts_type: "string",
  // },
  // id_r_run_number: {
  //   ts_type: "string",
  // },
  // id_r_test_type: {
  //   ts_type: "string",
  // },
  // r_additive_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_base_depth: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_base_form: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_completion_obs_no: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_completion_source: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_form_break_press: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_base_form_alias: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_case_name: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_frac_grad_act: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_frac_grad_des: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_frac_height_act: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_frac_height_des: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_frac_length_act: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_frac_length_des: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_frac_perm_act: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_frac_perm_des: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_frac_width_act: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_frac_width_des: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_instant_si_press_time1: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_instant_si_press_time2: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_instant_si_press_time3: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_instant_si_press_time4: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_interval_id: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_time1: {
  //   ts_type: "date",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_time2: {
  //   ts_type: "date",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_time3: {
  //   ts_type: "date",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_time4: {
  //   ts_type: "date",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_top_form_alias: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_gx_zone_id: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_injection_rate: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_instant_si_press: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_proppant_agent_amt: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_proppant_agent_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_proppant_amt_uom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_remark: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_row_changed_date: {
  //   ts_type: "date",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_run_number: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_source: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_stage_no: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_test_number: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_test_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_top_depth: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_top_form: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_treatment_amt: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_treatment_amt_uom: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_treatment_date: {
  //   ts_type: "date",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_treatment_fluid_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_treatment_obs_no: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_treatment_press: {
  //   ts_type: "number",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_treatment_status: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_treatment_type: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_uwi: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
  // r_well_test_source: {
  //   ts_type: "string",
  //   xform: "delimited_array_with_nulls",
  // },
};

const prefixes = {
  w_: "well",
  t_: "well_test",
  //r_: "well_treatment",
};

const asset_id_keys = [
  "w_uwi",
  "id_t_source",
  "id_t_test_type",
  "id_t_run_number",
];

const well_id_keys = ["w_uwi"];

const default_chunk = 100; // 100

///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter) => {
  return {
    asset_id_keys: asset_id_keys,
    default_chunk: default_chunk,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    sql: defineSQL(filter),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
