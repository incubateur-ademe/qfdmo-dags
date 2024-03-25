from airflow.models import DagBag

def test_dag_structure():
    dag_bag = DagBag(dag_folder='dags', include_examples=False)
    dag = dag_bag.get_dag('validate_and_process_dagruns')

    assert dag_bag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 4

    structure = {
        "branch_processing": ["skip_processing", "fetch_and_parse_data"],
        "fetch_and_parse_data": ["write_to_postgres"],
        "skip_processing": [],
        "write_to_postgres": [],
    }

    for task in dag.tasks:
        downstream_task_ids = [t.task_id for t in task.downstream_list]
        assert structure[task.task_id] == sorted(downstream_task_ids, reverse=True)
