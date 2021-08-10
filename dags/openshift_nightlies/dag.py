import sys
import os
import logging 
import json
from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.helpers import chain
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from textwrap import dedent

# Configure Path to have the Python Module on it
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from openshift_nightlies.models.dag_config import DagConfig
from openshift_nightlies.models.release import OpenshiftRelease, BaremetalRelease
from openshift_nightlies.tasks.install.cloud import openshift
from openshift_nightlies.tasks.install.openstack import jetpack
from openshift_nightlies.tasks.install.baremetal import jetski
from openshift_nightlies.tasks.benchmarks import e2e
from openshift_nightlies.tasks.utils import scale_ci_diagnosis
from openshift_nightlies.tasks.index import status
from openshift_nightlies.util import var_loader, manifest, constants
from abc import ABC, abstractmethod

# Set Task Logger to INFO for better task logs
log = logging.getLogger("airflow.task.operators")
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
log.addHandler(handler)

# This Applies to all DAGs
class AbstractOpenshiftNightlyDAG(ABC):
    def __init__(self, release: OpenshiftRelease, config: DagConfig):
        self.release = release
        self.config = config
        self.release_name = release.get_release_name()

        tags = []
        tags.append(self.release.platform)
        tags.append(self.release.release_stream)
        tags.append(self.release.profile)
        tags.append(self.release.version_alias)

        self.dag = DAG(
            self.release_name,
            default_args=self.config.default_args,
            tags=tags,
            description=f"DAG for Openshift Nightly builds {self.release_name}",
            schedule_interval=self.config.schedule_interval,
            max_active_runs=1,
            catchup=False
        )

        super().__init__()

    @abstractmethod
    def build(self):
        raise NotImplementedError()

    @abstractmethod
    def _get_openshift_installer(self):
        raise NotImplementedError()

    def _get_e2e_benchmarks(self): 
        return e2e.E2EBenchmarks(self.dag, self.release)

    def _get_scale_ci_diagnosis(self):
        return scale_ci_diagnosis.Diagnosis(self.dag, self.release)


class CloudOpenshiftNightlyDAG(AbstractOpenshiftNightlyDAG):
    def build(self):
        # installer = self._get_openshift_installer()
        # install_cluster = installer.get_install_task()
        
        with TaskGroup("utils", prefix_group_id=False, dag=self.dag) as utils:
            utils_tasks=self._get_scale_ci_diagnosis().get_utils()
            chain(*utils_tasks)

        with TaskGroup("benchmarks", prefix_group_id=False, dag=self.dag) as benchmarks:
            benchmark_tasks = self._get_e2e_benchmarks().get_benchmarks()
            chain(*benchmark_tasks)
            
        # eanup_cluster = installer.get_cleanup_task()
        benchmarks >> utils


    def _get_openshift_installer(self):
        return openshift.CloudOpenshiftInstaller(self.dag, self.release)



class BaremetalOpenshiftNightlyDAG(AbstractOpenshiftNightlyDAG):   
    def build(self):
        bm_installer = self._get_openshift_installer()
        install_cluster = bm_installer.get_install_task()
        scaleup_cluster = bm_installer.get_scaleup_task()
        install_cluster >> scaleup_cluster

    def _get_openshift_installer(self):
        return jetski.BaremetalOpenshiftInstaller(self.dag, self.release)



class OpenstackNightlyDAG(AbstractOpenshiftNightlyDAG):
    def build(self):
        installer = self._get_openshift_installer()
        install_cluster = installer.get_install_task()
        with TaskGroup("benchmarks", prefix_group_id=False, dag=self.dag) as benchmarks:
            benchmark_tasks = self._get_e2e_benchmarks().get_benchmarks()
            chain(*benchmark_tasks)

        if self.config.cleanup_on_success:
            cleanup_cluster = installer.get_cleanup_task()
            install_cluster >> benchmarks >> cleanup_cluster
        else: 
            install_cluster >> benchmarks

    def _get_openshift_installer(self):
        return jetpack.OpenstackJetpackInstaller(self.dag, self.release)



release_manifest = manifest.Manifest(constants.root_dag_dir)
for release in release_manifest.get_releases():
    openshift_release = release["release"]
    dag_config = release["config"]
    nightly = None
    if openshift_release.platform == "baremetal":
        nightly = BaremetalOpenshiftNightlyDAG(openshift_release, dag_config)
    elif openshift_release.platform == "openstack":
        nightly = OpenstackNightlyDAG(openshift_release, dag_config)
    else:
        nightly = CloudOpenshiftNightlyDAG(openshift_release, dag_config)
    
    nightly.build()
    globals()[nightly.release_name] = nightly.dag

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )

    # dag.doc_md = __doc__  # providing that you have a docstring at the beggining of the DAG
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
    )

    t1 >> [t2, t3]
