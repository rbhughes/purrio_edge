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
    s AS (
      SELECT
        core_id                    AS id_core_id,
        source                     AS id_source,
        uwi                        AS id_uwi,
        LIST(IFNULL(analysis_obs_no,          '${N}', CAST(analysis_obs_no AS VARCHAR)),          '${D}' ORDER BY core_id) AS s_analysis_obs_no,
        LIST(IFNULL(core_id,                  '${N}', CAST(core_id AS VARCHAR)),                  '${D}' ORDER BY core_id) AS s_core_id,
        LIST(IFNULL(gas_sat_vol,              '${N}', CAST(gas_sat_vol AS VARCHAR)),              '${D}' ORDER BY core_id) AS s_gas_sat_vol,
        LIST(IFNULL(grain_density,            '${N}', CAST(grain_density AS VARCHAR)),            '${D}' ORDER BY core_id) AS s_grain_density,
        LIST(IFNULL(grain_density_ouom,       '${N}', CAST(grain_density_ouom AS VARCHAR)),       '${D}' ORDER BY core_id) AS s_grain_density_ouom,
        LIST(IFNULL(gx_base_depth,            '${N}', CAST(gx_base_depth AS VARCHAR)),            '${D}' ORDER BY core_id) AS s_gx_base_depth,
        LIST(IFNULL(gx_bulk_density,          '${N}', CAST(gx_bulk_density AS VARCHAR)),          '${D}' ORDER BY core_id) AS s_gx_bulk_density,
        LIST(IFNULL(gx_formation,             '${N}', CAST(gx_formation AS VARCHAR)),             '${D}' ORDER BY core_id) AS s_gx_formation,
        LIST(IFNULL(gx_formation_alias,       '${N}', CAST(gx_formation_alias AS VARCHAR)),       '${D}' ORDER BY core_id) AS s_gx_formation_alias,
        LIST(IFNULL(gx_gamma_ray,             '${N}', CAST(gx_gamma_ray AS VARCHAR)),             '${D}' ORDER BY core_id) AS s_gx_gamma_ray,
        LIST(IFNULL(gx_lithology_desc,        '${N}', CAST(gx_lithology_desc AS VARCHAR)),        '${D}' ORDER BY core_id) AS s_gx_lithology_desc,
        LIST(IFNULL(gx_poissons_ratio,        '${N}', CAST(gx_poissons_ratio AS VARCHAR)),        '${D}' ORDER BY core_id) AS s_gx_poissons_ratio,
        LIST(IFNULL(gx_remark,                '${N}', CAST(gx_remark AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_remark,
        LIST(IFNULL(gx_resistivity,           '${N}', CAST(gx_resistivity AS VARCHAR)),           '${D}' ORDER BY core_id) AS s_gx_resistivity,
        LIST(IFNULL(gx_shift_depth,           '${N}', CAST(gx_shift_depth AS VARCHAR)),           '${D}' ORDER BY core_id) AS s_gx_shift_depth,
        LIST(IFNULL(gx_show_type,             '${N}', CAST(gx_show_type AS VARCHAR)),             '${D}' ORDER BY core_id) AS s_gx_show_type,
        LIST(IFNULL(gx_toc,                   '${N}', CAST(gx_toc AS VARCHAR)),                   '${D}' ORDER BY core_id) AS s_gx_toc,
        LIST(IFNULL(gx_total_clay,            '${N}', CAST(gx_total_clay AS VARCHAR)),            '${D}' ORDER BY core_id) AS s_gx_total_clay,
        LIST(IFNULL(gx_user1,                 '${N}', CAST(gx_user1 AS VARCHAR)),                 '${D}' ORDER BY core_id) AS s_gx_user1,
        LIST(IFNULL(gx_user10,                '${N}', CAST(gx_user10 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user10,
        LIST(IFNULL(gx_user100,               '${N}', CAST(gx_user100 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user100,
        LIST(IFNULL(gx_user101,               '${N}', CAST(gx_user101 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user101,
        LIST(IFNULL(gx_user102,               '${N}', CAST(gx_user102 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user102,
        LIST(IFNULL(gx_user103,               '${N}', CAST(gx_user103 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user103,
        LIST(IFNULL(gx_user104,               '${N}', CAST(gx_user104 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user104,
        LIST(IFNULL(gx_user105,               '${N}', CAST(gx_user105 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user105,
        LIST(IFNULL(gx_user106,               '${N}', CAST(gx_user106 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user106,
        LIST(IFNULL(gx_user107,               '${N}', CAST(gx_user107 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user107,
        LIST(IFNULL(gx_user108,               '${N}', CAST(gx_user108 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user108,
        LIST(IFNULL(gx_user109,               '${N}', CAST(gx_user109 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user109,
        LIST(IFNULL(gx_user11,                '${N}', CAST(gx_user11 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user11,
        LIST(IFNULL(gx_user110,               '${N}', CAST(gx_user110 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user110,
        LIST(IFNULL(gx_user111,               '${N}', CAST(gx_user111 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user111,
        LIST(IFNULL(gx_user112,               '${N}', CAST(gx_user112 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user112,
        LIST(IFNULL(gx_user113,               '${N}', CAST(gx_user113 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user113,
        LIST(IFNULL(gx_user114,               '${N}', CAST(gx_user114 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user114,
        LIST(IFNULL(gx_user115,               '${N}', CAST(gx_user115 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user115,
        LIST(IFNULL(gx_user116,               '${N}', CAST(gx_user116 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user116,
        LIST(IFNULL(gx_user117,               '${N}', CAST(gx_user117 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user117,
        LIST(IFNULL(gx_user118,               '${N}', CAST(gx_user118 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user118,
        LIST(IFNULL(gx_user119,               '${N}', CAST(gx_user119 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user119,
        LIST(IFNULL(gx_user12,                '${N}', CAST(gx_user12 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user12,
        LIST(IFNULL(gx_user120,               '${N}', CAST(gx_user120 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user120,
        LIST(IFNULL(gx_user121,               '${N}', CAST(gx_user121 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user121,
        LIST(IFNULL(gx_user122,               '${N}', CAST(gx_user122 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user122,
        LIST(IFNULL(gx_user123,               '${N}', CAST(gx_user123 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user123,
        LIST(IFNULL(gx_user124,               '${N}', CAST(gx_user124 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user124,
        LIST(IFNULL(gx_user125,               '${N}', CAST(gx_user125 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user125,
        LIST(IFNULL(gx_user126,               '${N}', CAST(gx_user126 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user126,
        LIST(IFNULL(gx_user127,               '${N}', CAST(gx_user127 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user127,
        LIST(IFNULL(gx_user128,               '${N}', CAST(gx_user128 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user128,
        LIST(IFNULL(gx_user129,               '${N}', CAST(gx_user129 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user129,
        LIST(IFNULL(gx_user13,                '${N}', CAST(gx_user13 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user13,
        LIST(IFNULL(gx_user130,               '${N}', CAST(gx_user130 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user130,
        LIST(IFNULL(gx_user131,               '${N}', CAST(gx_user131 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user131,
        LIST(IFNULL(gx_user132,               '${N}', CAST(gx_user132 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user132,
        LIST(IFNULL(gx_user133,               '${N}', CAST(gx_user133 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user133,
        LIST(IFNULL(gx_user134,               '${N}', CAST(gx_user134 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user134,
        LIST(IFNULL(gx_user135,               '${N}', CAST(gx_user135 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user135,
        LIST(IFNULL(gx_user136,               '${N}', CAST(gx_user136 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user136,
        LIST(IFNULL(gx_user137,               '${N}', CAST(gx_user137 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user137,
        LIST(IFNULL(gx_user138,               '${N}', CAST(gx_user138 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user138,
        LIST(IFNULL(gx_user139,               '${N}', CAST(gx_user139 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user139,
        LIST(IFNULL(gx_user14,                '${N}', CAST(gx_user14 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user14,
        LIST(IFNULL(gx_user140,               '${N}', CAST(gx_user140 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user140,
        LIST(IFNULL(gx_user141,               '${N}', CAST(gx_user141 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user141,
        LIST(IFNULL(gx_user142,               '${N}', CAST(gx_user142 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user142,
        LIST(IFNULL(gx_user143,               '${N}', CAST(gx_user143 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user143,
        LIST(IFNULL(gx_user144,               '${N}', CAST(gx_user144 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user144,
        LIST(IFNULL(gx_user145,               '${N}', CAST(gx_user145 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user145,
        LIST(IFNULL(gx_user146,               '${N}', CAST(gx_user146 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user146,
        LIST(IFNULL(gx_user147,               '${N}', CAST(gx_user147 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user147,
        LIST(IFNULL(gx_user148,               '${N}', CAST(gx_user148 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user148,
        LIST(IFNULL(gx_user149,               '${N}', CAST(gx_user149 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user149,
        LIST(IFNULL(gx_user15,                '${N}', CAST(gx_user15 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user15,
        LIST(IFNULL(gx_user150,               '${N}', CAST(gx_user150 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user150,
        LIST(IFNULL(gx_user151,               '${N}', CAST(gx_user151 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user151,
        LIST(IFNULL(gx_user152,               '${N}', CAST(gx_user152 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user152,
        LIST(IFNULL(gx_user153,               '${N}', CAST(gx_user153 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user153,
        LIST(IFNULL(gx_user154,               '${N}', CAST(gx_user154 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user154,
        LIST(IFNULL(gx_user155,               '${N}', CAST(gx_user155 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user155,
        LIST(IFNULL(gx_user156,               '${N}', CAST(gx_user156 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user156,
        LIST(IFNULL(gx_user157,               '${N}', CAST(gx_user157 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user157,
        LIST(IFNULL(gx_user158,               '${N}', CAST(gx_user158 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user158,
        LIST(IFNULL(gx_user159,               '${N}', CAST(gx_user159 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user159,
        LIST(IFNULL(gx_user16,                '${N}', CAST(gx_user16 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user16,
        LIST(IFNULL(gx_user160,               '${N}', CAST(gx_user160 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user160,
        LIST(IFNULL(gx_user161,               '${N}', CAST(gx_user161 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user161,
        LIST(IFNULL(gx_user162,               '${N}', CAST(gx_user162 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user162,
        LIST(IFNULL(gx_user163,               '${N}', CAST(gx_user163 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user163,
        LIST(IFNULL(gx_user164,               '${N}', CAST(gx_user164 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user164,
        LIST(IFNULL(gx_user165,               '${N}', CAST(gx_user165 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user165,
        LIST(IFNULL(gx_user166,               '${N}', CAST(gx_user166 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user166,
        LIST(IFNULL(gx_user167,               '${N}', CAST(gx_user167 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user167,
        LIST(IFNULL(gx_user168,               '${N}', CAST(gx_user168 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user168,
        LIST(IFNULL(gx_user169,               '${N}', CAST(gx_user169 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user169,
        LIST(IFNULL(gx_user17,                '${N}', CAST(gx_user17 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user17,
        LIST(IFNULL(gx_user170,               '${N}', CAST(gx_user170 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user170,
        LIST(IFNULL(gx_user171,               '${N}', CAST(gx_user171 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user171,
        LIST(IFNULL(gx_user172,               '${N}', CAST(gx_user172 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user172,
        LIST(IFNULL(gx_user173,               '${N}', CAST(gx_user173 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user173,
        LIST(IFNULL(gx_user174,               '${N}', CAST(gx_user174 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user174,
        LIST(IFNULL(gx_user175,               '${N}', CAST(gx_user175 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user175,
        LIST(IFNULL(gx_user176,               '${N}', CAST(gx_user176 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user176,
        LIST(IFNULL(gx_user177,               '${N}', CAST(gx_user177 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user177,
        LIST(IFNULL(gx_user178,               '${N}', CAST(gx_user178 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user178,
        LIST(IFNULL(gx_user179,               '${N}', CAST(gx_user179 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user179,
        LIST(IFNULL(gx_user18,                '${N}', CAST(gx_user18 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user18,
        LIST(IFNULL(gx_user180,               '${N}', CAST(gx_user180 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user180,
        LIST(IFNULL(gx_user181,               '${N}', CAST(gx_user181 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user181,
        LIST(IFNULL(gx_user182,               '${N}', CAST(gx_user182 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user182,
        LIST(IFNULL(gx_user183,               '${N}', CAST(gx_user183 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user183,
        LIST(IFNULL(gx_user184,               '${N}', CAST(gx_user184 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user184,
        LIST(IFNULL(gx_user185,               '${N}', CAST(gx_user185 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user185,
        LIST(IFNULL(gx_user186,               '${N}', CAST(gx_user186 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user186,
        LIST(IFNULL(gx_user187,               '${N}', CAST(gx_user187 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user187,
        LIST(IFNULL(gx_user188,               '${N}', CAST(gx_user188 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user188,
        LIST(IFNULL(gx_user189,               '${N}', CAST(gx_user189 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user189,
        LIST(IFNULL(gx_user19,                '${N}', CAST(gx_user19 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user19,
        LIST(IFNULL(gx_user190,               '${N}', CAST(gx_user190 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user190,
        LIST(IFNULL(gx_user191,               '${N}', CAST(gx_user191 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user191,
        LIST(IFNULL(gx_user192,               '${N}', CAST(gx_user192 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user192,
        LIST(IFNULL(gx_user193,               '${N}', CAST(gx_user193 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user193,
        LIST(IFNULL(gx_user194,               '${N}', CAST(gx_user194 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user194,
        LIST(IFNULL(gx_user195,               '${N}', CAST(gx_user195 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user195,
        LIST(IFNULL(gx_user196,               '${N}', CAST(gx_user196 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user196,
        LIST(IFNULL(gx_user197,               '${N}', CAST(gx_user197 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user197,
        LIST(IFNULL(gx_user198,               '${N}', CAST(gx_user198 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user198,
        LIST(IFNULL(gx_user199,               '${N}', CAST(gx_user199 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user199,
        LIST(IFNULL(gx_user2,                 '${N}', CAST(gx_user2 AS VARCHAR)),                 '${D}' ORDER BY core_id) AS s_gx_user2,
        LIST(IFNULL(gx_user20,                '${N}', CAST(gx_user20 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user20,
        LIST(IFNULL(gx_user200,               '${N}', CAST(gx_user200 AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_gx_user200,
        LIST(IFNULL(gx_user21,                '${N}', CAST(gx_user21 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user21,
        LIST(IFNULL(gx_user22,                '${N}', CAST(gx_user22 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user22,
        LIST(IFNULL(gx_user23,                '${N}', CAST(gx_user23 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user23,
        LIST(IFNULL(gx_user24,                '${N}', CAST(gx_user24 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user24,
        LIST(IFNULL(gx_user25,                '${N}', CAST(gx_user25 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user25,
        LIST(IFNULL(gx_user26,                '${N}', CAST(gx_user26 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user26,
        LIST(IFNULL(gx_user27,                '${N}', CAST(gx_user27 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user27,
        LIST(IFNULL(gx_user28,                '${N}', CAST(gx_user28 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user28,
        LIST(IFNULL(gx_user29,                '${N}', CAST(gx_user29 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user29,
        LIST(IFNULL(gx_user3,                 '${N}', CAST(gx_user3 AS VARCHAR)),                 '${D}' ORDER BY core_id) AS s_gx_user3,
        LIST(IFNULL(gx_user30,                '${N}', CAST(gx_user30 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user30,
        LIST(IFNULL(gx_user31,                '${N}', CAST(gx_user31 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user31,
        LIST(IFNULL(gx_user32,                '${N}', CAST(gx_user32 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user32,
        LIST(IFNULL(gx_user33,                '${N}', CAST(gx_user33 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user33,
        LIST(IFNULL(gx_user34,                '${N}', CAST(gx_user34 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user34,
        LIST(IFNULL(gx_user35,                '${N}', CAST(gx_user35 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user35,
        LIST(IFNULL(gx_user36,                '${N}', CAST(gx_user36 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user36,
        LIST(IFNULL(gx_user37,                '${N}', CAST(gx_user37 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user37,
        LIST(IFNULL(gx_user38,                '${N}', CAST(gx_user38 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user38,
        LIST(IFNULL(gx_user39,                '${N}', CAST(gx_user39 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user39,
        LIST(IFNULL(gx_user4,                 '${N}', CAST(gx_user4 AS VARCHAR)),                 '${D}' ORDER BY core_id) AS s_gx_user4,
        LIST(IFNULL(gx_user40,                '${N}', CAST(gx_user40 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user40,
        LIST(IFNULL(gx_user41,                '${N}', CAST(gx_user41 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user41,
        LIST(IFNULL(gx_user42,                '${N}', CAST(gx_user42 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user42,
        LIST(IFNULL(gx_user43,                '${N}', CAST(gx_user43 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user43,
        LIST(IFNULL(gx_user44,                '${N}', CAST(gx_user44 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user44,
        LIST(IFNULL(gx_user45,                '${N}', CAST(gx_user45 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user45,
        LIST(IFNULL(gx_user46,                '${N}', CAST(gx_user46 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user46,
        LIST(IFNULL(gx_user47,                '${N}', CAST(gx_user47 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user47,
        LIST(IFNULL(gx_user48,                '${N}', CAST(gx_user48 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user48,
        LIST(IFNULL(gx_user49,                '${N}', CAST(gx_user49 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user49,
        LIST(IFNULL(gx_user5,                 '${N}', CAST(gx_user5 AS VARCHAR)),                 '${D}' ORDER BY core_id) AS s_gx_user5,
        LIST(IFNULL(gx_user50,                '${N}', CAST(gx_user50 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user50,
        LIST(IFNULL(gx_user51,                '${N}', CAST(gx_user51 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user51,
        LIST(IFNULL(gx_user52,                '${N}', CAST(gx_user52 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user52,
        LIST(IFNULL(gx_user53,                '${N}', CAST(gx_user53 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user53,
        LIST(IFNULL(gx_user54,                '${N}', CAST(gx_user54 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user54,
        LIST(IFNULL(gx_user55,                '${N}', CAST(gx_user55 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user55,
        LIST(IFNULL(gx_user56,                '${N}', CAST(gx_user56 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user56,
        LIST(IFNULL(gx_user57,                '${N}', CAST(gx_user57 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user57,
        LIST(IFNULL(gx_user58,                '${N}', CAST(gx_user58 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user58,
        LIST(IFNULL(gx_user59,                '${N}', CAST(gx_user59 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user59,
        LIST(IFNULL(gx_user6,                 '${N}', CAST(gx_user6 AS VARCHAR)),                 '${D}' ORDER BY core_id) AS s_gx_user6,
        LIST(IFNULL(gx_user60,                '${N}', CAST(gx_user60 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user60,
        LIST(IFNULL(gx_user61,                '${N}', CAST(gx_user61 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user61,
        LIST(IFNULL(gx_user62,                '${N}', CAST(gx_user62 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user62,
        LIST(IFNULL(gx_user63,                '${N}', CAST(gx_user63 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user63,
        LIST(IFNULL(gx_user64,                '${N}', CAST(gx_user64 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user64,
        LIST(IFNULL(gx_user65,                '${N}', CAST(gx_user65 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user65,
        LIST(IFNULL(gx_user66,                '${N}', CAST(gx_user66 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user66,
        LIST(IFNULL(gx_user67,                '${N}', CAST(gx_user67 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user67,
        LIST(IFNULL(gx_user68,                '${N}', CAST(gx_user68 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user68,
        LIST(IFNULL(gx_user69,                '${N}', CAST(gx_user69 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user69,
        LIST(IFNULL(gx_user7,                 '${N}', CAST(gx_user7 AS VARCHAR)),                 '${D}' ORDER BY core_id) AS s_gx_user7,
        LIST(IFNULL(gx_user70,                '${N}', CAST(gx_user70 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user70,
        LIST(IFNULL(gx_user71,                '${N}', CAST(gx_user71 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user71,
        LIST(IFNULL(gx_user72,                '${N}', CAST(gx_user72 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user72,
        LIST(IFNULL(gx_user73,                '${N}', CAST(gx_user73 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user73,
        LIST(IFNULL(gx_user74,                '${N}', CAST(gx_user74 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user74,
        LIST(IFNULL(gx_user75,                '${N}', CAST(gx_user75 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user75,
        LIST(IFNULL(gx_user76,                '${N}', CAST(gx_user76 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user76,
        LIST(IFNULL(gx_user77,                '${N}', CAST(gx_user77 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user77,
        LIST(IFNULL(gx_user78,                '${N}', CAST(gx_user78 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user78,
        LIST(IFNULL(gx_user79,                '${N}', CAST(gx_user79 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user79,
        LIST(IFNULL(gx_user8,                 '${N}', CAST(gx_user8 AS VARCHAR)),                 '${D}' ORDER BY core_id) AS s_gx_user8,
        LIST(IFNULL(gx_user80,                '${N}', CAST(gx_user80 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user80,
        LIST(IFNULL(gx_user81,                '${N}', CAST(gx_user81 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user81,
        LIST(IFNULL(gx_user82,                '${N}', CAST(gx_user82 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user82,
        LIST(IFNULL(gx_user83,                '${N}', CAST(gx_user83 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user83,
        LIST(IFNULL(gx_user84,                '${N}', CAST(gx_user84 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user84,
        LIST(IFNULL(gx_user85,                '${N}', CAST(gx_user85 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user85,
        LIST(IFNULL(gx_user86,                '${N}', CAST(gx_user86 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user86,
        LIST(IFNULL(gx_user87,                '${N}', CAST(gx_user87 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user87,
        LIST(IFNULL(gx_user88,                '${N}', CAST(gx_user88 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user88,
        LIST(IFNULL(gx_user89,                '${N}', CAST(gx_user89 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user89,
        LIST(IFNULL(gx_user9,                 '${N}', CAST(gx_user9 AS VARCHAR)),                 '${D}' ORDER BY core_id) AS s_gx_user9,
        LIST(IFNULL(gx_user90,                '${N}', CAST(gx_user90 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user90,
        LIST(IFNULL(gx_user91,                '${N}', CAST(gx_user91 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user91,
        LIST(IFNULL(gx_user92,                '${N}', CAST(gx_user92 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user92,
        LIST(IFNULL(gx_user93,                '${N}', CAST(gx_user93 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user93,
        LIST(IFNULL(gx_user94,                '${N}', CAST(gx_user94 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user94,
        LIST(IFNULL(gx_user95,                '${N}', CAST(gx_user95 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user95,
        LIST(IFNULL(gx_user96,                '${N}', CAST(gx_user96 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user96,
        LIST(IFNULL(gx_user97,                '${N}', CAST(gx_user97 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user97,
        LIST(IFNULL(gx_user98,                '${N}', CAST(gx_user98 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user98,
        LIST(IFNULL(gx_user99,                '${N}', CAST(gx_user99 AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_gx_user99,
        LIST(IFNULL(gx_vitrinite_reflectance, '${N}', CAST(gx_vitrinite_reflectance AS VARCHAR)), '${D}' ORDER BY core_id) AS s_gx_vitrinite_reflectance,
        LIST(IFNULL(gx_youngs_modulus,        '${N}', CAST(gx_youngs_modulus AS VARCHAR)),        '${D}' ORDER BY core_id) AS s_gx_youngs_modulus,
        LIST(IFNULL(k90,                      '${N}', CAST(k90 AS VARCHAR)),                      '${D}' ORDER BY core_id) AS s_k90,
        LIST(IFNULL(k90_ouom,                 '${N}', CAST(k90_ouom AS VARCHAR)),                 '${D}' ORDER BY core_id) AS s_k90_ouom,
        LIST(IFNULL(kmax,                     '${N}', CAST(kmax AS VARCHAR)),                     '${D}' ORDER BY core_id) AS s_kmax,
        LIST(IFNULL(kmax_ouom,                '${N}', CAST(kmax_ouom AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_kmax_ouom,
        LIST(IFNULL(kvert,                    '${N}', CAST(kvert AS VARCHAR)),                    '${D}' ORDER BY core_id) AS s_kvert,
        LIST(IFNULL(kvert_ouom,               '${N}', CAST(kvert_ouom AS VARCHAR)),               '${D}' ORDER BY core_id) AS s_kvert_ouom,
        LIST(IFNULL(oil_sat,                  '${N}', CAST(oil_sat AS VARCHAR)),                  '${D}' ORDER BY core_id) AS s_oil_sat,
        LIST(IFNULL(pore_volume_oil_sat,      '${N}', CAST(pore_volume_oil_sat AS VARCHAR)),      '${D}' ORDER BY core_id) AS s_pore_volume_oil_sat,
        LIST(IFNULL(pore_volume_water_sat,    '${N}', CAST(pore_volume_water_sat AS VARCHAR)),    '${D}' ORDER BY core_id) AS s_pore_volume_water_sat,
        LIST(IFNULL(porosity,                 '${N}', CAST(porosity AS VARCHAR)),                 '${D}' ORDER BY core_id) AS s_porosity,
        LIST(IFNULL(row_changed_date,         '${N}', CAST(row_changed_date AS VARCHAR)),         '${D}' ORDER BY core_id) AS s_row_changed_date,
        LIST(IFNULL(sample_id,                '${N}', CAST(sample_id AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_sample_id,
        LIST(IFNULL(sample_number,            '${N}', CAST(sample_number AS VARCHAR)),            '${D}' ORDER BY core_id) AS s_sample_number,
        LIST(IFNULL(source,                   '${N}', CAST(source AS VARCHAR)),                   '${D}' ORDER BY core_id) AS s_source,
        LIST(IFNULL(top_depth,                '${N}', CAST(top_depth AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_top_depth,
        LIST(IFNULL(top_depth_ouom,           '${N}', CAST(top_depth_ouom AS VARCHAR)),           '${D}' ORDER BY core_id) AS s_top_depth_ouom,
        LIST(IFNULL(uwi,                      '${N}', CAST(uwi AS VARCHAR)),                      '${D}' ORDER BY core_id) AS s_uwi,
        LIST(IFNULL(water_sat,                '${N}', CAST(water_sat AS VARCHAR)),                '${D}' ORDER BY core_id) AS s_water_sat
      FROM well_core_sample_anal
      GROUP BY id_uwi, id_source, id_core_id
    )
    SELECT
      w.*,
      c.base_depth                  AS c_base_depth,
      c.base_depth_ouom             AS c_base_depth_ouom,
      c.core_id                     AS c_core_id,
      c.core_type                   AS c_core_type,
      c.gx_primary_core_form_alias  AS c_gx_primary_core_form_alias,
      c.gx_qualifying_field         AS c_gx_qualifying_field,
      c.gx_remark                   AS c_gx_remark,
      c.gx_user1                    AS c_gx_user1,
      c.primary_core_form           AS c_primary_core_form,
      c.recovered_amt               AS c_recovered_amt,
      c.recovery_amt_ouom           AS c_recovery_amt_ouom,
      c.recovery_date               AS c_recovery_date,
      c.reported_core_number        AS c_reported_core_number,
      c.row_changed_date            AS c_row_changed_date,
      c.source                      AS c_source,
      c.top_depth                   AS c_top_depth,
      c.top_depth_ouom              AS c_top_depth_ouom,
      c.uwi                         AS c_uwi,
      s.*
    FROM well_core c
    JOIN s
      ON c.uwi = s.id_uwi
      AND c.source = s.id_source
      AND c.core_id = s.id_core_id
    JOIN w ON w.w_uwi = s.id_uwi
    ) x`;

  const order = `ORDER BY w_uwi`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  //const fast_count = `SELECT COUNT(DISTINCT wellid) AS count FROM gx_well_curve`;

  return {
    select: select,
    count: count,
    order: order,
    where: where,
  };
};

const xformer = (args) => {
  let { func, key, typ, arg, obj } = args;

  const ensureType = (type: string, val: any) => {
    if (val == null) {
      return null;
    } else if (type === "object") {
      console.log("UNEXPECTED OBJECT TYPE! (needs xformer)", type);
      console.log(val);
      return null;
    } else if (type === "string") {
      //return decodeWin1252(val)
      return val.replace(/[\u0000-\u001F\u007F-\u009F]/g, "");
    } else if (type === "number") {
      // cuz blank strings (\t\r\n) evaluate to 0
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

  const D = "|&|";
  const N = "purrNULL";

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

  // WELL_CORE_SAMPLE_ANAL

  id_core_id: {
    ts_type: "string",
  },
  id_source: {
    ts_type: "string",
  },
  id_uwi: {
    ts_type: "string",
  },
  s_analysis_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_core_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gas_sat_vol: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_grain_density: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_grain_density_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_base_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_bulk_density: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_formation: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_formation_alias: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_gamma_ray: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_lithology_desc: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_poissons_ratio: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_resistivity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_shift_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_show_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_toc: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_total_clay: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user1: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user10: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user100: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user101: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user102: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user103: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user104: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user105: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user106: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user107: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user108: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user109: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user11: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user110: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user111: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user112: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user113: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user114: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user115: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user116: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user117: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user118: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user119: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user12: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user120: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user121: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user122: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user123: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user124: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user125: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user126: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user127: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user128: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user129: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user13: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user130: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user131: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user132: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user133: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user134: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user135: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user136: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user137: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user138: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user139: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user14: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user140: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user141: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user142: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user143: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user144: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user145: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user146: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user147: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user148: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user149: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user15: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user150: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user151: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user152: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user153: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user154: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user155: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user156: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user157: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user158: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user159: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user16: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user160: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user161: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user162: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user163: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user164: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user165: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user166: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user167: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user168: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user169: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user17: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user170: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user171: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user172: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user173: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user174: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user175: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user176: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user177: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user178: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user179: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user18: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user180: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user181: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user182: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user183: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user184: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user185: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user186: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user187: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user188: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user189: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user19: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user190: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user191: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user192: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user193: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user194: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user195: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user196: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user197: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user198: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user199: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user2: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user20: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user200: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user21: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user22: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user23: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user24: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user25: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user26: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user27: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user28: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user29: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user3: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user30: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user31: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user32: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user33: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user34: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user35: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user36: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user37: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user38: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user39: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user4: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user40: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user41: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user42: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user43: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user44: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user45: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user46: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user47: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user48: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user49: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user5: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user50: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user51: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user52: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user53: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user54: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user55: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user56: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user57: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user58: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user59: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user6: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user60: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user61: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user62: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user63: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user64: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user65: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user66: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user67: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user68: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user69: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user7: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user70: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user71: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user72: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user73: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user74: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user75: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user76: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user77: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user78: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user79: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user8: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user80: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user81: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user82: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user83: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user84: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user85: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user86: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user87: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user88: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user89: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user9: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user90: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user91: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user92: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user93: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user94: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user95: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user96: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user97: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user98: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_user99: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_vitrinite_reflectance: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_youngs_modulus: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_k90: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_k90_ouom: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_kmax: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_kmax_ouom: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_kvert: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_kvert_ouom: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_oil_sat: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_pore_volume_oil_sat: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_pore_volume_water_sat: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_porosity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  s_sample_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_sample_number: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_top_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_top_depth_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_uwi: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_water_sat: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },

  // WELL_CORE

  c_base_depth: {
    ts_type: "number",
  },
  c_base_depth_ouom: {
    ts_type: "string",
  },
  c_core_id: {
    ts_type: "string",
  },
  c_core_type: {
    ts_type: "string",
  },
  c_gx_primary_core_form_alias: {
    ts_type: "string",
  },
  c_gx_qualifying_field: {
    ts_type: "string",
  },
  c_gx_remark: {
    ts_type: "string",
  },
  c_gx_user1: {
    ts_type: "string",
  },
  c_primary_core_form: {
    ts_type: "string",
  },
  c_recovered_amt: {
    ts_type: "number",
  },
  c_recovery_amt_ouom: {
    ts_type: "string",
  },
  c_recovery_date: {
    ts_type: "date",
  },
  c_reported_core_number: {
    ts_type: "string",
  },
  c_row_changed_date: {
    ts_type: "date",
  },
  c_source: {
    ts_type: "string",
  },
  c_top_depth: {
    ts_type: "number",
  },
  c_top_depth_ouom: {
    ts_type: "string",
  },
  c_uwi: {
    ts_type: "string",
  },
};

const prefixes = {
  w_: "well",
  s_: "well_core_sample_anal",
  c_: "well_core",
};

const global_id_keys = ["w_uwi", "c_source", "c_core_id"];

const well_id_keys = ["w_uwi"];

const pg_cols = ["id", "repo_id", "well_id", "geo_type", "tag", "doc"];

const default_chunk = 1000;

///////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter) => {
  return {
    default_chunk: default_chunk,
    global_id_keys: global_id_keys,
    pg_cols: pg_cols,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    sql: defineSQL(filter),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
