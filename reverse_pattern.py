def folder_monitor():
    """
    Note: If you are using this API the file ingestion should follow below keypoints
    1. folder_structure table need to be filled in io_configuration db
    2. For shared folder path: inside tenant folder of /app/input priority_folder column declared folders should be created
    3. Inside those create as many folders or structures you want the final folder where the file should be picked shoud have `input` name
    """

    data = request.json
    logging.info(f'Data is: {data}')
    # data = data['data']
    tenant_id = data.get('tenant_id', None)
    
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
        
    
    trace_id = generate_random_64bit_string()

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id if tenant_id is not None else ''
    )

    with zipkin_span(
            service_name='folder_monitor',
            zipkin_attrs=attr,
            span_name='folder_monitor',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        try:
            logging.debug(f'Connecting to tenant {tenant_id}')

            db_config['tenant_id'] = tenant_id
            db = DB('io_configuration', **db_config)

            # input_config = db.get_all(
            #     'input_configuration', condition={'active': 0})
            input_config_qry = f'select * from `input_configuration`'
            input_config=db.execute(input_config_qry)
            output_config = db.get_all('output_configuration')

            # input_config = db.execute_(input_config).set_index('id')
            # #output_config = db.get_all('output_configuration')
            # output_config = f'select * from `output_configuration`'
            # output_config = db.execute_(output_config).set_index('id')

            logging.debug(f'Input Config: {input_config.to_dict()}')
            logging.debug(f'Output Config: {output_config.to_dict()}')

            # Sanity checks
            if (input_config.loc[input_config['type'] == 'Document'].empty
                    or output_config.loc[output_config['type'] == 'Document'].empty):
                message = 'Input/Output not configured in DB.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            for index, row in input_config.iterrows():
                input_path = row['access_1']
                output_path = output_config.ix[row['output']]['access_1']
                workflow = row['workflow']

                logging.debug(f'Input path: {input_path}')
                logging.debug(f'Output path: {output_path}')

                if (input_path is None or not input_path
                        or output_path is None or not output_path):
                    message = 'Input/Output is empty/none in DB.'
                    logging.error(message)
                    return jsonify({'flag': False, 'message': message})

                input_path_str = "/app/input/"+input_path
                output_path = f"/app/output/{tenant_id}/assets/pdf/"+output_path
                input_path = Path(input_path_str)
                output_path = Path(output_path)
                logging.debug(f'Input Absolute path: {input_path}')
                logging.debug(f'Output Absolute path: {output_path}')

                reponse_data = {}

                # Only watch the folder if both are valid directory
                if input_path.is_dir():
                    logging.debug(str(input_path) + '/*')
                    # files = Path(input_path).rglob('*')
                    all_files = get_priority_files(input_path_str)
                    logging.info(f"All files found are: {all_files}")

                    # get first file and send it to pick as priority need to be upload first
                    if len(all_files):
                        files = [all_files[0]]
                    else:
                        files = []

                    file_names = []
                    for file_ in files:
                        logging.debug(f'move from: {file_}')
                        reponse_data['move_from'] = str(file_)
                        filename = file_.name.replace(' ', '_')
                        logging.debug(
                            f'move to: {str(output_path / filename)}')

                        # creating a copy file in error folder and
                        # will be deleted when queue assigned in camunda_api
                        copy_to = Path(file_).parents[1] / 'error/'
                        logging.debug(f'Creating a copy at {copy_to}')
                        shutil.copy(Path(file_), copy_to)

                        shutil.move(Path(file_), Path(output_path) / filename)

                        # Audit
                        # stats_db = DB('stats', **db_config)
                        # query = 'Insert into `audit` (`type`, `last_modified_by`, `changed_data`,`reference_column`,`reference_value`,`table_name`) values (%s,%s,%s,%s,%s,%s)'
                        # params = ['Receiving', 'folder_monitor',
                        #           json.dumps({"queue": "New file received"}), 'filename', filename, 'File Pickup']
                        # stats_db.execute(query, params=params)

                        file_names.append({'file_name': filename})

                    logging.debug(f'Files: {file_names}')
                    reponse_data['files'] = file_names
                    reponse_data['workflow'] = workflow

                    final_response_data = {"flag": True, "data": reponse_data}
                else:
                    message = f'{input_path} not a directory'
                    logging.error(message)
                    final_response_data = {'flag': True, 'message': message}
        except:
            logging.exception(
                'Something went wrong watching folder. Check trace.')
            final_response_data = {'flag': False, 'message': 'System error! Please contact your system administrator.'}
    try:
        memory_after = measure_memory_usage()
        memory_consumed = (memory_after - memory_before) / \
            (1024 * 1024 * 1024)
        end_time = tt()
        memory_consumed = f"{memory_consumed:.10f}"
        logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
        time_consumed = str(round(end_time-start_time,3))
    except:
        logging.warning("Failed to calc end of ram and time")
        logging.exception("ram calc went wrong")
        memory_consumed = None
        time_consumed = None
        pass

    # insert audit
    audit_data = {"tenant_id": tenant_id, "user": "", "case_id": "",
                    "api_service": "folder_monitor", "service_container": "folder_monitor_api",
                    "changed_data": json.dumps({"queue": "New file received"}),"tables_involved": "","memory_usage_gb": str(memory_consumed), 
                    "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                    "response_data": reponse_data, "trace_id": trace_id,
                    "session_id": "","status":str(final_response_data['flag'])}
    insert_into_audit("New File Received", audit_data)

    return jsonify(final_response_data)

