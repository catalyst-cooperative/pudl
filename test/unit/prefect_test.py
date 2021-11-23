"""Test prefect helper functions."""
from prefect import Flow, task

from pudl.etl import set_downstream_checkpoints


class TestPrefectCaching:
    """Test Prefect caching behavior."""

    def test_set_downstream_checkpoints(self):
        """Test downstream checkpoints are false if task checkpoint is false."""
        @task
        def a():
            print("a")

        @task(checkpoint=False)
        def b():
            print("b")

        @task
        def c(a, b):
            print("c")

        @task
        def d(c):
            print("d")

        with Flow("hello-flow") as flow:
            a = a()
            b = b()
            c = c(a, b)
            d = d(c)

        root_tasks = flow.root_tasks()
        for root_task in root_tasks:
            set_downstream_checkpoints(flow, root_task)

        assert flow.get_tasks("a")[0].checkpoint is None
        assert flow.get_tasks("b")[0].checkpoint is False
        assert flow.get_tasks("c")[0].checkpoint is False
        assert flow.get_tasks("d")[0].checkpoint is False

    def test_set_downstream_checkpoints_collision(self):
        """
        Test downstream checkpoints are false if task checkpoint is false.

        Ensure nothing strange happens if multiple tasks that share downstream tasks
        both are not checkpointing.
        """
        @task(checkpoint=False)
        def a():
            print("a")

        @task(checkpoint=False)
        def b():
            print("b")

        @task
        def c(a, b):
            print("c")

        @task
        def d(c):
            print("d")

        with Flow("hello-flow") as flow:
            a = a()
            b = b()
            c = c(a, b)
            d = d(c)

        root_tasks = flow.root_tasks()
        for root_task in root_tasks:
            set_downstream_checkpoints(flow, root_task)

        assert flow.get_tasks("a")[0].checkpoint is False
        assert flow.get_tasks("b")[0].checkpoint is False
        assert flow.get_tasks("c")[0].checkpoint is False
        assert flow.get_tasks("d")[0].checkpoint is False
