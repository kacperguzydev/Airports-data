# tracing.py

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
import config

_tracer = None

def init_tracer():
    global _tracer
    if _tracer is not None:
        return _tracer

    resource = Resource.create({"service.name": "airports-pipeline"})
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    jaeger_exporter = JaegerExporter(
        agent_host_name=config.OTEL_EXPORTER_JAEGER_AGENT_HOST,
        agent_port=config.OTEL_EXPORTER_JAEGER_AGENT_PORT,
    )
    span_processor = BatchSpanProcessor(jaeger_exporter)
    provider.add_span_processor(span_processor)

    _tracer = trace.get_tracer(__name__)
    return _tracer
