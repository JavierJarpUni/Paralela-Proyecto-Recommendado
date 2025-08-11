# main.py - Cloud Function principal
import functions_framework
from google.cloud import storage
import json
import uuid
from datetime import datetime
import asyncio
import aiohttp

@functions_framework.http
def bigdata_api_gateway(request):
    """
    API Gateway principal para el sistema de procesamiento
    """
    if request.method == 'OPTIONS':
        # Manejo CORS
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        }
        return ('', 204, headers)
    
    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
        if request.method == 'POST' and request.path == '/process':
            return start_processing(request, headers)
        elif request.method == 'GET' and request.path.startswith('/status/'):
            job_id = request.path.split('/')[-1]
            return get_processing_status(job_id, headers)
        elif request.method == 'GET' and request.path.startswith('/results/'):
            job_id = request.path.split('/')[-1] 
            return get_processing_results(job_id, headers)
        else:
            return ({'error': 'Endpoint not found'}, 404, headers)
            
    except Exception as e:
        return ({'error': str(e)}, 500, headers)

def start_processing(request, headers):
    """
    Iniciar procesamiento de datos
    """
    request_json = request.get_json()
    
    if not request_json or 'data' not in request_json:
        return ({'error': 'No data provided'}, 400, headers)
    
    # Generar job ID único
    job_id = str(uuid.uuid4())
    
    # Validar datos básicamente
    data = request_json['data']
    if not isinstance(data, list) or len(data) == 0:
        return ({'error': 'Invalid data format'}, 400, headers)
    
    if len(data) > 1000000:
        return ({'error': 'Data too large (max 1M elements)'}, 400, headers)
    
    # Crear entrada en el tracking
    tracking_info = {
        'job_id': job_id,
        'status': 'initiated',
        'created_at': datetime.utcnow().isoformat(),
        'data_size': len(data),
        'stages': {
            'validation': 'pending',
            'gpu_processing': 'pending', 
            'spark_rdd': 'pending',
            'spark_dataframe': 'pending',
            'analysis': 'pending'
        }
    }
    
    # Guardar en Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket('bigdata-processing-results')
    tracking_blob = bucket.blob(f'tracking/{job_id}.json')
    tracking_blob.upload_from_string(json.dumps(tracking_info))
    
    # Llamar al sistema de actores (a través de Cloud Run)
    asyncio.run(trigger_actor_system(job_id, data))
    
    return ({
        'job_id': job_id,
        'status': 'initiated',
        'message': 'Processing started successfully',
        'estimated_time_minutes': estimate_processing_time(len(data))
    }, 200, headers)

async def trigger_actor_system(job_id, data):
    """
    Disparar el sistema de actores
    """
    async with aiohttp.ClientSession() as session:
        payload = {
            'job_id': job_id,
            'data': data,
            'action': 'start_processing'
        }
        
        async with session.post(
            'https://actor-system-service-url/start',
            json=payload,
            headers={'Content-Type': 'application/json'}
        ) as response:
            return await response.json()

def get_processing_status(job_id, headers):
    """
    Obtener estado del procesamiento
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket('bigdata-processing-results')
        
        # Obtener tracking info
        tracking_blob = bucket.blob(f'tracking/{job_id}.json')
        if not tracking_blob.exists():
            return ({'error': 'Job not found'}, 404, headers)
        
        tracking_data = json.loads(tracking_blob.download_as_text())
        
        # Calcular progreso
        stages = tracking_data.get('stages', {})
        completed_stages = sum(1 for status in stages.values() if status == 'completed')
        total_stages = len(stages)
        progress_percentage = (completed_stages / total_stages) * 100
        
        return ({
            'job_id': job_id,
            'status': tracking_data.get('status', 'unknown'),
            'progress_percentage': progress_percentage,
            'stages': stages,
            'created_at': tracking_data.get('created_at'),
            'updated_at': tracking_data.get('updated_at'),
            'estimated_completion': tracking_data.get('estimated_completion')
        }, 200, headers)
        
    except Exception as e:
        return ({'error': f'Failed to get status: {str(e)}'}, 500, headers)

def get_processing_results(job_id, headers):
    """
    Obtener resultados del procesamiento
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket('bigdata-processing-results')
        
        # Verificar que el job esté completo
        tracking_blob = bucket.blob(f'tracking/{job_id}.json')
        if not tracking_blob.exists():
            return ({'error': 'Job not found'}, 404, headers)
        
        tracking_data = json.loads(tracking_blob.download_as_text())
        
        if tracking_data.get('status') != 'completed':
            return ({
                'error': 'Processing not completed yet',
                'current_status': tracking_data.get('status', 'unknown')
            }, 202, headers)
        
        # Cargar resultados de todas las etapas
        results = {}
        
        # GPU Processing results
        gpu_blob = bucket.blob(f'gpu_processed/{job_id}.json')
        if gpu_blob.exists():
            results['gpu_processing'] = json.loads(gpu_blob.download_as_text())
        
        # Spark RDD results
        rdd_blob = bucket.blob(f'spark_rdd/{job_id}.json')
        if rdd_blob.exists():
            results['spark_rdd'] = json.loads(rdd_blob.download_as_text())
        
        # Spark DataFrame results
        df_blob = bucket.blob(f'spark_dataframe/{job_id}.json')
        if df_blob.exists():
            results['spark_dataframe'] = json.loads(df_blob.download_as_text())
        
        # Analysis results
        analysis_blob = bucket.blob(f'analysis/{job_id}.json')
        if analysis_blob.exists():
            results['performance_analysis'] = json.loads(analysis_blob.download_as_text())
        
        return ({
            'job_id': job_id,
            'status': 'completed',
            'results': results,
            'processing_summary': generate_summary(results)
        }, 200, headers)
        
    except Exception as e:
        return ({'error': f'Failed to get results: {str(e)}'}, 500, headers)

def generate_summary(results):
    """
    Generar resumen de resultados
    """
    summary = {
        'total_processing_stages': len(results),
        'performance_metrics': {}
    }
    
    if 'spark_rdd' in results and 'spark_dataframe' in results:
        rdd_time = results['spark_rdd'].get('processing_time_ms', 0)
        df_time = results['spark_dataframe'].get('processing_time_ms', 0)
        
        if rdd_time > 0 and df_time > 0:
            speedup = rdd_time / df_time if df_time < rdd_time else df_time / rdd_time
            faster_method = 'DataFrame' if df_time < rdd_time else 'RDD'
            
            summary['performance_metrics'] = {
                'rdd_processing_time_ms': rdd_time,
                'dataframe_processing_time_ms': df_time,
                'speedup_ratio': round(speedup, 2),
                'faster_method': faster_method,
                'performance_improvement': f"{round((speedup - 1) * 100, 1)}% faster with {faster_method}"
            }
    
    return summary

def estimate_processing_time(data_size):
    """
    Estimar tiempo de procesamiento basado en el tamaño de datos
    """
    # Estimación basada en experiencia: ~1 segundo por 10k elementos
    base_time = data_size / 10000
    # Agregar overhead de inicialización y network
    overhead = 2
    return max(1, int(base_time + overhead))