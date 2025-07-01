import warnings

from typing import Sequence, ClassVar, Callable, Collection, Mapping, Any
from airflow.sdk.bases.decorator import DecoratedOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
from airflow.utils.context import context_merge
from airflow.utils.operator_helpers import determine_kwargs
from airflow.sdk.definitions.context import Context
from airflow.sdk.bases.decorator import task_decorator_factory, TaskDecorator

class _SQLDecoratedOperator(DecoratedOperator, SQLExecuteQueryOperator):
    
    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *SQLExecuteQueryOperator.template_fields)
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
        **SQLExecuteQueryOperator.template_fields_renderers,
    }

    custom_operator_name:str = "@task.sql"
    overwrite_rtif_after_execution: bool = True
    
    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                f"The `multiple_outputs` parameter is deprecated and will be removed in a future version of the SDK. Use the `output_name` parameter instead.",
                UserWarning,
                stacklevel=3,
            )
            
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            sql=SET_DURING_EXECUTION,
            multiple_outputs=False,
            **kwargs,
        )
        
    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        
        self.sql = self.python_callable(*self.op_args, **kwargs)
        
        if not isinstance(self.sql, str) or self.sql.strip() == "":
            raise TypeError("The `sql` parameter must be a non-empty string.")
        
        context["ti"].render_templates()
        
        return super().execute(context)


def sql_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_SQLDecoratedOperator,
        **kwargs,
    )
