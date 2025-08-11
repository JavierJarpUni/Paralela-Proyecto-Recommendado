# metal_processor_function.py
import functions_framework
from google.cloud import storage
import json
import subprocess
import os
import tempfile

@functions_framework.http
def metal_gpu_processor(request):
    """
    Cloud Function que procesa arrays usando Metal GPU
    """
    try:
        request_json = request.get_json()
        
        if not request_json or 'data' not in request_json:
            return {'error': 'No data provided'}, 400
            
        input_data = request_json['data']
        processing_type = request_json.get('type', 'normalize')
        
        # Crear archivo temporal con los datos
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump({'data': input_data, 'type': processing_type}, temp_file)
            temp_path = temp_file.name
        
        try:
            # Ejecutar el procesador Swift
            result = subprocess.run([
                'swift', 'run', 'MetalProcessor', temp_path
            ], capture_output=True, text=True, cwd='/tmp/metal_processor')
            
            if result.returncode != 0:
                return {'error': f'Processing failed: {result.stderr}'}, 500
            
            # Parsear resultado
            processed_data = json.loads(result.stdout)
            
            # Guardar resultado en Cloud Storage
            storage_client = storage.Client()
            bucket = storage_client.bucket('bigdata-processing-results')
            
            job_id = request_json.get('job_id', 'unknown')
            blob = bucket.blob(f'gpu_processed/{job_id}.json')
            blob.upload_from_string(json.dumps(processed_data))
            
            return {
                'status': 'success',
                'job_id': job_id,
                'processed_count': len(processed_data.get('result', [])),
                'storage_path': f'gs://bigdata-processing-results/gpu_processed/{job_id}.json'
            }
            
        finally:
            os.unlink(temp_path)
            
    except Exception as e:
        return {'error': str(e)}, 500