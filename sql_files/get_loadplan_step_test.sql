select *
from snp_lp_step
where i_load_plan = {loadplan_id}
    --i_load_plan=45502 --and par_i_lp_step=676502
order by i_lp_step, nvl(par_i_lp_step,0), step_order