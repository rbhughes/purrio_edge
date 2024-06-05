const purr_recent = "__purrRECENT__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

const select = `SELECT * FROM (
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
      LIST(IFNULL(base_depth,                 '${purr_null}', CAST(base_depth AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS t_base_depth,
      LIST(IFNULL(base_form,                  '${purr_null}', CAST(base_form AS VARCHAR)),                 '${purr_delimiter}' ORDER BY test_number)  AS t_base_form,
      LIST(IFNULL(bottom_choke_desc,          '${purr_null}', CAST(bottom_choke_desc AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS t_bottom_choke_desc,
      LIST(IFNULL(casing_press,               '${purr_null}', CAST(casing_press AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS t_casing_press,
      LIST(IFNULL(choke_size_desc,            '${purr_null}', CAST(choke_size_desc AS VARCHAR)),           '${purr_delimiter}' ORDER BY test_number)  AS t_choke_size_desc,
      LIST(IFNULL(flow_press,                 '${purr_null}', CAST(flow_press AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS t_flow_press,
      LIST(IFNULL(flow_temp,                  '${purr_null}', CAST(flow_temp AS VARCHAR)),                 '${purr_delimiter}' ORDER BY test_number)  AS t_flow_temp,
      LIST(IFNULL(gas_flow_amt,               '${purr_null}', CAST(gas_flow_amt AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS t_gas_flow_amt,
      LIST(IFNULL(gas_flow_amt_uom,           '${purr_null}', CAST(gas_flow_amt_uom AS VARCHAR)),          '${purr_delimiter}' ORDER BY test_number)  AS t_gas_flow_amt_uom,
      LIST(IFNULL(gor,                        '${purr_null}', CAST(gor AS VARCHAR)),                       '${purr_delimiter}' ORDER BY test_number)  AS t_gor,
      LIST(IFNULL(gx_base_form_alias,         '${purr_null}', CAST(gx_base_form_alias AS VARCHAR)),        '${purr_delimiter}' ORDER BY test_number)  AS t_gx_base_form_alias,
      LIST(IFNULL(gx_bhp_z,                   '${purr_null}', CAST(gx_bhp_z AS VARCHAR)),                  '${purr_delimiter}' ORDER BY test_number)  AS t_gx_bhp_z,
      LIST(IFNULL(gx_co2_pct,                 '${purr_null}', CAST(gx_co2_pct AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS t_gx_co2_pct,
      LIST(IFNULL(gx_conversion_factor,       '${purr_null}', CAST(gx_conversion_factor AS VARCHAR)),      '${purr_delimiter}' ORDER BY test_number)  AS t_gx_conversion_factor,
      LIST(IFNULL(gx_cushion_type,            '${purr_null}', CAST(gx_cushion_type AS VARCHAR)),           '${purr_delimiter}' ORDER BY test_number)  AS t_gx_cushion_type,
      LIST(IFNULL(gx_cushion_vol,             '${purr_null}', CAST(gx_cushion_vol AS VARCHAR)),            '${purr_delimiter}' ORDER BY test_number)  AS t_gx_cushion_vol,
      LIST(IFNULL(gx_cushion_vol_ouom,        '${purr_null}', CAST(gx_cushion_vol_ouom AS VARCHAR)),       '${purr_delimiter}' ORDER BY test_number)  AS t_gx_cushion_vol_ouom,
      LIST(IFNULL(gx_ft_flow_amt,             '${purr_null}', CAST(gx_ft_flow_amt AS VARCHAR)),            '${purr_delimiter}' ORDER BY test_number)  AS t_gx_ft_flow_amt,
      LIST(IFNULL(gx_ft_flow_amt_uom,         '${purr_null}', CAST(gx_ft_flow_amt_uom AS VARCHAR)),        '${purr_delimiter}' ORDER BY test_number)  AS t_gx_ft_flow_amt_uom,
      LIST(IFNULL(gx_gas_cum,                 '${purr_null}', CAST(gx_gas_cum AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS t_gx_gas_cum,
      LIST(IFNULL(gx_max_ft_flow_rate,        '${purr_null}', CAST(gx_max_ft_flow_rate AS VARCHAR)),       '${purr_delimiter}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate,
      LIST(IFNULL(gx_max_ft_flow_rate_uom,    '${purr_null}', CAST(gx_max_ft_flow_rate_uom AS VARCHAR)),   '${purr_delimiter}' ORDER BY test_number)  AS t_gx_max_ft_flow_rate_uom,
      LIST(IFNULL(gx_recorder_depth,          '${purr_null}', CAST(gx_recorder_depth AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS t_gx_recorder_depth,
      LIST(IFNULL(gx_top_form_alias,          '${purr_null}', CAST(gx_top_form_alias AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS t_gx_top_form_alias,
      LIST(IFNULL(gx_tracer,                  '${purr_null}', CAST(gx_tracer AS VARCHAR)),                 '${purr_delimiter}' ORDER BY test_number)  AS t_gx_tracer,
      LIST(IFNULL(gx_z_factor,                '${purr_null}', CAST(gx_z_factor AS VARCHAR)),               '${purr_delimiter}' ORDER BY test_number)  AS t_gx_z_factor,
      LIST(IFNULL(h2s_pct,                    '${purr_null}', CAST(h2s_pct AS VARCHAR)),                   '${purr_delimiter}' ORDER BY test_number)  AS t_h2s_pct,
      LIST(IFNULL(max_gas_flow_rate,          '${purr_null}', CAST(max_gas_flow_rate AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS t_max_gas_flow_rate,
      LIST(IFNULL(max_hydrostatic_press,      '${purr_null}', CAST(max_hydrostatic_press AS VARCHAR)),     '${purr_delimiter}' ORDER BY test_number)  AS t_max_hydrostatic_press,
      LIST(IFNULL(max_oil_flow_rate,          '${purr_null}', CAST(max_oil_flow_rate AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS t_max_oil_flow_rate,
      LIST(IFNULL(max_water_flow_rate,        '${purr_null}', CAST(max_water_flow_rate AS VARCHAR)),       '${purr_delimiter}' ORDER BY test_number)  AS t_max_water_flow_rate,
      LIST(IFNULL(oil_flow_amt,               '${purr_null}', CAST(oil_flow_amt AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS t_oil_flow_amt,
      LIST(IFNULL(oil_flow_amt_uom,           '${purr_null}', CAST(oil_flow_amt_uom AS VARCHAR)),          '${purr_delimiter}' ORDER BY test_number)  AS t_oil_flow_amt_uom,
      LIST(IFNULL(oil_gravity,                '${purr_null}', CAST(oil_gravity AS VARCHAR)),               '${purr_delimiter}' ORDER BY test_number)  AS t_oil_gravity,
      LIST(IFNULL(primary_fluid_recovered,    '${purr_null}', CAST(primary_fluid_recovered AS VARCHAR)),   '${purr_delimiter}' ORDER BY test_number)  AS t_primary_fluid_recovered,
      LIST(IFNULL(production_method,          '${purr_null}', CAST(production_method AS VARCHAR)),         '${purr_delimiter}' ORDER BY test_number)  AS t_production_method,
      LIST(IFNULL(remark,                     '${purr_null}', CAST(remark AS VARCHAR)),                    '${purr_delimiter}' ORDER BY test_number)  AS t_remark,
      LIST(IFNULL(report_temp,                '${purr_null}', CAST(report_temp AS VARCHAR)),               '${purr_delimiter}' ORDER BY test_number)  AS t_report_temp,
      LIST(IFNULL(row_changed_date,           '${purr_null}', CAST(row_changed_date AS VARCHAR)),          '${purr_delimiter}' ORDER BY test_number)  AS t_row_changed_date,
      LIST(IFNULL(run_number,                 '${purr_null}', CAST(run_number AS VARCHAR)),                '${purr_delimiter}' ORDER BY test_number)  AS t_run_number,
      LIST(IFNULL(shut_off_type,              '${purr_null}', CAST(shut_off_type AS VARCHAR)),             '${purr_delimiter}' ORDER BY test_number)  AS t_shut_off_type,
      LIST(IFNULL(source,                     '${purr_null}', CAST(source AS VARCHAR)),                    '${purr_delimiter}' ORDER BY test_number)  AS t_source,
      LIST(IFNULL(test_date,                  '${purr_null}', CAST(test_date AS VARCHAR)),                 '${purr_delimiter}' ORDER BY test_number)  AS t_test_date,
      LIST(IFNULL(test_duration,              '${purr_null}', CAST(test_duration AS VARCHAR)),             '${purr_delimiter}' ORDER BY test_number)  AS t_test_duration,
      LIST(IFNULL(test_number,                '${purr_null}', CAST(test_number AS VARCHAR)),               '${purr_delimiter}' ORDER BY test_number)  AS t_test_number,
      LIST(IFNULL(test_subtype,               '${purr_null}', CAST(test_subtype AS VARCHAR)),              '${purr_delimiter}' ORDER BY test_number)  AS t_test_subtype,
      LIST(IFNULL(test_type,                  '${purr_null}', CAST(test_type AS VARCHAR)),                 '${purr_delimiter}' ORDER BY test_number)  AS t_test_type,
      LIST(IFNULL(top_choke_desc,             '${purr_null}', CAST(top_choke_desc AS VARCHAR)),            '${purr_delimiter}' ORDER BY test_number)  AS t_top_choke_desc,
      LIST(IFNULL(top_depth,                  '${purr_null}', CAST(top_depth AS VARCHAR)),                 '${purr_delimiter}' ORDER BY test_number)  AS t_top_depth,
      LIST(IFNULL(top_form,                   '${purr_null}', CAST(top_form AS VARCHAR)),                  '${purr_delimiter}' ORDER BY test_number)  AS t_top_form,
      LIST(IFNULL(uwi,                        '${purr_null}', CAST(uwi AS VARCHAR)),                       '${purr_delimiter}' ORDER BY test_number)  AS t_uwi,
      LIST(IFNULL(water_flow_amt,             '${purr_null}', CAST(water_flow_amt AS VARCHAR)),            '${purr_delimiter}' ORDER BY test_number)  AS t_water_flow_amt,
      LIST(IFNULL(water_flow_amt_uom,         '${purr_null}', CAST(water_flow_amt_uom AS VARCHAR)),        '${purr_delimiter}' ORDER BY test_number)  AS t_water_flow_amt_uom
    FROM well_test
    WHERE test_type = 'IP'
    ${purr_recent}
    GROUP BY uwi, source, test_type, run_number
  )
  SELECT
    w.*,
    t.*
  FROM w
  JOIN t ON w.w_uwi = t.id_t_uwi
) x`;

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

const asset_id_keys = [
  "w_uwi",
  "id_t_source",
  "id_t_test_type",
  "id_t_run_number",
];

const default_chunk = 100; // 100

const notes = ["Removed WELL_TREATMENT to save memory"];

const order = `ORDER BY w_uwi, id_t_source, id_t_test_type, id_t_run_number`;

const prefixes = {
  w_: "well",
  t_: "well_test",
  //r_: "well_treatment",
};


const well_id_keys = ["w_uwi"];


///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = () => {
  return {
    asset_id_keys,
    default_chunk,
    notes,
    order,
    purr_delimiter,
    purr_null,
    purr_recent,
    prefixes,
    select,
    well_id_keys,
    xforms
  };
};
