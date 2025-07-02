@dag.op(retry_policy=ex_retry_policy, tags={'resource_queue': 'extract_queue'})
async def extract_slfd_sales_r2():
    ''' Extract and Load from SLFD CSVs '''
    total_dfs = await util.csv_locs_to_dfs(csv_mapping=jconfig.slfd_sales_csv_map)
    for df_map in total_dfs:
        df_map['df'].columns = get_col_names(df_map['table_name'])
    return await df_to_sql_r2(total_dfs)


@dag.op(retry_policy=ex_retry_policy, tags={'resource_queue': 'extract_queue'})
async def extract_slfd_adv_r2():
    ''' Extract and Load from Top Advisors Excel '''
    top_advisor_df = pd.read_excel(
        jconfig.top_advisor_path + jconfig.top_advisor_filename,
        sheet_name=jconfig.top_advisor_tabname,
        na_filter=False,
        dtype=str
    )
    dfs = [{
        'file_name': jconfig.top_advisor_filename,
        'table_name': jconfig.top_advisor_tablename,
        'df': top_advisor_df,
        'fast_execute_flag': True
    }]
    return await df_to_sql_r2(dfs)


@dag.op(retry_policy=ex_retry_policy, tags={'resource_queue': 'sqlserver_queue'})
async def load_oda_r2() -> tp.List[tp.Dict]:
    ''' Extracts from queries on ODA '''
    query_files = os.listdir(jconfig.oda_queries_path)
    query_mappings = {}
    for query_file in query_files:
        file_name = query_file.rpartition('.')[0]
        query_mappings[file_name] = {'source': jconfig.oda_query_src}
    total_dfs = await util.queries_to_dfs(
        query_path=jconfig.oda_queries_path,
        query_mapping=query_mappings
    )
    for df_map in total_dfs:
        print(df_map['table_name'])
        df_map['df'].columns = get_col_names(df_map['table_name'])
    return total_dfs


@dag.op(tags={'resource_queue': 'sqlserver_queue'})
def load_oda_create_r2():
    ''' Runs multi-statement queries on ODA '''
    ps_command = f"""
    Import-Module {pconfig.project_dag_dir}isds_daily_pipeline/powershell/Load-SQLtoSQLServer -Force
    $SQL_ODA = '{pconfig.project_dag_dir}isds_daily_pipeline/sql_queries/ODA_create/'
    $DB_SERVER_DEV = 'gdmtdev1.ca.sunlife,50130'
    $DB = '{iconfig.conn_details[util.target_conn().get('conn_name')]['database']}'
    ."{jconfig.oda_ps}"
    """
    util.logger.info(ps_command)
    try:
        p = subprocess.Popen(["powershell", ps_command], stdout=subprocess.PIPE)
        raw_outputs = p.communicate()
        outputs = (raw_outputs[0].decode('utf-8')).split('\n')
        success = None
        for output in outputs:
            if 'ERROR' in output.upper():
                util.logger.error(output)
                success = False
            elif 'WARN' in output.upper():
                util.logger.warn(output)
            else:
                util.logger.info(output)
                success = True
        if not success:
            raise Exception(f"ps_script {jconfig.oda_ps} exited with reported errors or warnings")
        return dag.Nothing
    except subprocess.CalledProcessError as expt:
        err_log = [out.decode('utf-8') for out in expt.output.split(b'\r\n')]
        for err_txt in err_log:
            util.logger.debug(err_txt)
        raise expt


@dag.op(tags={'resource_queue': 'sqlserver_queue'})
async def load_gdmt_r2():
    query_files = os.listdir(jconfig.gdmt_queries_path)
    query_mappings = {}
    for query_file in query_files:
        file_name = query_file.rpartition('.')[0]
        query_mappings[file_name] = {'source': jconfig.gdmt_query_src}
    total_dfs = await util.queries_to_dfs(
        query_path=jconfig.gdmt_queries_path,
        query_mapping=query_mappings
    )
    for df_map in total_dfs:
        df_map['df'].columns = get_col_names(df_map['table_name'])
    return await df_to_sql_r2(total_dfs)


@dag.op(tags={'resource_queue': 'sqlserver_queue'})
async def load_fixes_r2(d):
    query_files = os.listdir(jconfig.fixes_queries_path)
    query_mappings = {}
    for query_file in query_files:
        file_name = query_file.rpartition('.')[0]
        query_mappings[file_name] = {
            'source': util.target_conn(job_prod_flag=jconfig.prod_flag).get('conn_name')
        }
    await util.execute_queries(query_path=jconfig.fixes_queries_path, query_mapping=query_mappings)
    return dag.Nothing


@dag.op(tags={'resource_queue': 'compute_queue'})
def dbt_setup_r2(d):
    util.dbt_command_stream(
        command=jconfig.dbt_compile,
        prod_flag=util.target_conn(job_prod_flag=jconfig.prod_flag).get('dbt')
    )
    return dag.Nothing


@dag.op(tags={'resource_queue': 'sqlserver_queue'})
def load_dbt_seed(d):
    util.logger.info(jconfig.dbt_seed)
    util.dbt_command_stream(
        command=jconfig.dbt_seed,
        prod_flag=util.target_conn(job_prod_flag=jconfig.prod_flag).get('dbt')
    )
    return dag.Nothing


@dag.op(tags={'resource_queue': 'compute_queue'})
def dbt_src_test(d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12):
    ''' Runs dbt tests on all Source tables for early warning of failed conditions '''
    util.logger.info(jconfig.dbt_test_sources)
    util.dbt_command_stream(
        command=jconfig.dbt_test_sources,
        prod_flag=util.target_conn(job_prod_flag=jconfig.prod_flag).get('dbt')
    )
    return dag.Nothing


@dag.op(tags={'resource_queue': 'sqlserver_queue'})
def projection_setup_r3(d):
    util.logger.info(jconfig.build_projections)
    util.dbt_command(
        command=jconfig.build_projections,
        prod_flag=util.target_conn(job_prod_flag=jconfig.prod_flag).get('dbt')
    )
    return dag.Nothing


@dag.op(tags={'resource_queue': 'compute_queue'})
def py_projections_r3(d):
    conn_key = util.target_conn(job_prod_flag=jconfig.prod_flag).get('conn_name')
    conn_db = iconfig.conn_details.get(conn_key).get('database')
    if not os.path.isdir(jconfig.proj_output_path):
        os.mkdir(jconfig.proj_output_path)
    util.logger.debug(f"target path: {jconfig.proj_output_path}")
    try:
        util.logger.debug(f"starting subprocess for run_projections.py ...")
        command = f"cd {pconfig.project_dag_dir}; {pconfig.python_exe} {jconfig.proj_py_path}/run_projections.py {conn_db} {jconfig.proj_output_full_path}"
        util.logger.debug(command)
        p = subprocess.Popen(["powershell", command], stdout=subprocess.PIPE)
        raw_outputs = p.communicate()
        outputs = (raw_outputs[0].decode('utf-8')).split('\n')
        success = None
        for output in outputs:
            if 'ERROR' in output.upper():
                util.logger.error(output)
                success = False
            elif 'WARN' in output.upper():
                util.logger.warn(output)
            else:
                util.logger.debug(output)
                success = True
        if not success:
            raise Exception("projections script exited with reported errors or warnings")
        else:
            util.logger.info("success")
        return outputs
    except subprocess.CalledProcessError as expt:
        err_log = [out.decode('utf-8') for out in expt.output.split(b'\r\n')]
        for err_txt in err_log:
            util.logger.debug(err_txt)
        raise expt


@dag.op(tags={'resource_queue': 'sqlserver_queue'})
async def load_projections_r3(d):
    if not os.path.exists(os.path.dirname(jconfig.proj_output_path)):
        os.makedirs(os.path.dirname(jconfig.proj_output_path))
    df_map = await util.csv_to_df(
        file=jconfig.proj_output_filename,
        path=jconfig.proj_output_path,
        file_mapping=jconfig.proj_output_map
    )
    df_map['df'].columns = get_col_names(jconfig.proj_output_map[jconfig.proj_output_filename]['table_name'])
    return await df_to_sql_r2([df_map])


@dag.op(tags={'resource_queue': 'compute_queue'})
def dbt_main_r3(d):
    ''' Executes core logic for Daily Sales Pipeline data model via dbt build Selected graph based on dbt tag: daily '''
    util.dbt_command_stream(
        command=jconfig.dbt_main_cmd,
        prod_flag=util.target_conn(job_prod_flag=jconfig.prod_flag).get('dbt')
    )
    return dag.Nothing


@dag.op
def db2_to_csv_production(d):
    try:
        os.makedirs(jconfig.csv_path)
        print("Temp folder created for db2 output csv: " + jconfig.csv_path)
    except FileExistsError:
        print("Temp folder already exists for db2 output csv: " + jconfig.csv_path)
        pass

    ps_command = f'''
    $MVS_USERNAME = "{jconfig.MVS_USERNAME}"
    $MVS_PASSWORD = "{jconfig.MVS_PASSWORD}"
    $CSV_OTHER="{jconfig.csv_path}"
    '''

    ps_full_path = jconfig.db2_ps_path + jconfig.db2_ps_script
    util.logger.debug(ps_full_path)
    try:
        p = subprocess.Popen(["powershell", f'{ps_command}; ."{ps_full_path}"'], stdout=subprocess.PIPE)
        raw_outputs = p.communicate()
        outputs = (raw_outputs[0].decode('utf-8')).split('/n')
        success = None
        for output in outputs:
            if 'ERROR' in output.upper():
                util.logger.error(output)
                success = False
            elif 'WARN' in output.upper():
                util.logger.warn(output)
            else:
                util.logger.info(output)
                success = True
        if not success:
            raise Exception(f"ps_script {jconfig.db2_ps_script} exited with reported errors or warnings")
        util.logger.info(f"script {jconfig.db2_ps_script} completed without errors")
        return dag.Nothing
    except subprocess.CalledProcessError as expt:
        err_log = [out.decode('utf-8') for out in expt.output.split(b'\r\n')]
        for err_txt in err_log:
            util.logger.debug(err_txt)
        raise expt


@dag.op(tags={'resource_queue': 'sqlserver_queue'})
def csv_to_sql_optimized_production(dep):
    output_schema = 'isds_landing'
    job_prod_flag = pconfig.prod_flag
    output_conn_name = 'GDMT_PROD' if job_prod_flag == 1 else 'GDMT_DEV'

    for query_file in os.listdir(jconfig.csv_path):
        file_name = jconfig.csv_path + '/' + query_file
        table_name = query_file[0:-4]
        util.csv_to_sql_optimized(
            input_file=file_name,
            dest_name=output_conn_name,
            dest_schema=output_schema,
            dest_table=table_name
        )
    return dag.Nothing


@dag.op(tags={'resource_queue': 'sqlserver_queue'})
async def get_phi_data():
    my_df_table = pd.read_excel(jconfig.PHI_data_file, sheet_name='data')
    output_conn_name = 'GDMT_PROD' if pconfig.prod_flag == 1 else 'GDMT_DEV'
    await util.df_to_sql(
        {'df': my_df_table, 'table_name': 'idfin_phi_daily', 'file_name': 'PHI sales database 2023.xls', 'fast_execute_flag': 1},
        output_conn_name,
        'isds_landing'
    )
    return dag.Nothing


@dag.graph
def daily_pipeline_r2():
    ''' Second revision of the execution graph 2022-10 '''
    edl_load = [
        aws_distribution_prod_to_sql(extract_aws_distribution_prod()),
        aws_salesforce_dm_to_sql(extract_aws_salesforce_dm()),
        aws_sfdc_to_sql(extract_aws_sfdc())
    ]
    align_load = df_to_sql_limited(extract_align_r2())
    retro_load = extract_retro_r2()
    limra_load = extract_limra_r2()
    HNW_load = extract_HNW()
    sales_load = extract_slfd_sales_r2()
    adv_load = extract_slfd_adv_r2()
    db2_load = csv_to_sql_optimized_production(db2_to_csv_production(get_phi_data()))

    oda_load = df_to_sql_limited(load_oda_r2())
    oda_create = load_oda_create_r2()
    gdmt_load = load_gdmt_r2()
    sfdc_fixes = load_fixes_r2(edl_load)
    dbt_setup = load_dbt_seed(dbt_setup_r2(util.identify_run()))
    dbt_ready = dbt_src_test(
        dbt_setup, sfdc_fixes,
        sales_load, adv_load, align_load, retro_load, limra_load, HNW_load, db2_load, gdmt_load, oda_load, oda_create
    )
    proj_load = load_projections_r3(py_projections_r3(projection_setup_r3(dbt_ready)))
    main = dbt_main_r3(proj_load)

daily_pipeline_r2_job = daily_pipeline_r2.to_job(
    executor_def=dag.multi_or_in_process_executor,
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 3,
                    "tag_concurrency_limits": [
                        {"key": "resource_queue", "value": "extract_queue", "limit": 1},
                        {"key": "resource_queue", "value": "trino_queue", "limit": 1},
                        {"key": "resource_queue", "value": "sqlserver_queue", "limit": 1},
                        {"key": "resource_queue", "value": "compute_queue", "limit": 1},
                    ]
                }
            }
        }
    },
    tags={"dagster/priority": "1"}
)

daily_pipeline_r2_prod_schedule = dag.ScheduleDefinition(
    job=daily_pipeline_r2_job,
    cron_schedule='35 9 * * *',
    default_status=dag.DefaultScheduleStatus.RUNNING,
    execution_timezone='America/New_York'
)
