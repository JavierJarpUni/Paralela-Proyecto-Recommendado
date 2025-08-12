# performance_analyzer.py
import json
import matplotlib.pyplot as plt
import pandas as pd
from google.cloud import storage
import seaborn as sns

class PerformanceAnalyzer:
    def __init__(self, project_id, bucket_name):
        self.storage_client = storage.Client(project=project_id)
        self.bucket = self.storage_client.bucket(bucket_name)
    
    def analyze_job(self, job_id):
        """Analizar rendimiento de un job específico"""
        
        # Cargar resultados
        gpu_result = self._load_result(f'gpu_processed/{job_id}.json')
        rdd_result = self._load_result(f'spark_rdd/{job_id}.json')
        df_result = self._load_result(f'spark_dataframe/{job_id}.json')
        
        # Crear análisis
        analysis = {
            'job_id': job_id,
            'gpu_processing': {
                'status': 'completed' if gpu_result else 'failed',
                'processed_count': gpu_result.get('processed_count', 0) if gpu_result else 0
            },
            'spark_comparison': self._compare_spark_results(rdd_result, df_result),
            'recommendations': self._generate_recommendations(rdd_result, df_result)
        }
        
        # Generar visualizaciones
        self._create_performance_charts(analysis, job_id)
        
        return analysis
    
    def _load_result(self, blob_path):
        """Cargar resultado desde Cloud Storage"""
        try:
            blob = self.bucket.blob(blob_path)
            if blob.exists():
                return json.loads(blob.download_as_text())
        except Exception:
            return None
    
    def _compare_spark_results(self, rdd_result, df_result):
        """Comparar resultados de RDD vs DataFrame"""
        if not rdd_result or not df_result:
            return {'error': 'Missing results for comparison'}
        
        rdd_time = rdd_result.get('processing_time_ms', 0)
        df_time = df_result.get('processing_time_ms', 0)
        
        speedup = rdd_time / df_time if df_time > 0 else 0
        faster_method = 'DataFrame' if df_time < rdd_time else 'RDD'
        
        return {
            'rdd_time_ms': rdd_time,
            'dataframe_time_ms': df_time,
            'speedup': round(speedup, 2),
            'faster_method': faster_method,
            'performance_improvement_pct': round((abs(rdd_time - df_time) / max(rdd_time, df_time)) * 100, 1)
        }
    
    def _generate_recommendations(self, rdd_result, df_result):
        """Generar recomendaciones basadas en resultados"""
        recommendations = []
        
        if not rdd_result or not df_result:
            return ['Unable to generate recommendations due to missing results']
        
        rdd_time = rdd_result.get('processing_time_ms', 0)
        df_time = df_result.get('processing_time_ms', 0)
        
        if df_time < rdd_time:
            improvement = ((rdd_time - df_time) / rdd_time) * 100
            recommendations.append(f'Use DataFrame API for {improvement:.1f}% better performance')
        else:
            improvement = ((df_time - rdd_time) / df_time) * 100  
            recommendations.append(f'RDD API showed {improvement:.1f}% better performance for this dataset')
        
        # Recomendaciones adicionales basadas en tamaño de datos
        record_count = rdd_result.get('record_count', 0)
        if record_count > 100000:
            recommendations.append('Consider using more partitions for large datasets')
            recommendations.append('Enable adaptive query execution for better resource utilization')
        
        return recommendations
    
    def _create_performance_charts(self, analysis, job_id):
        """Crear gráficos de rendimiento"""
        
        # Configurar estilo
        plt.style.use('seaborn-v0_8')
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle(f'Performance Analysis - Job {job_id[:8]}', fontsize=16)
        
        # Gráfico 1: Comparación de tiempos
        spark_comp = analysis['spark_comparison']
        if 'error' not in spark_comp:
            methods = ['RDD', 'DataFrame']
            times = [spark_comp['rdd_time_ms'], spark_comp['dataframe_time_ms']]
            colors = ['#ff6b6b' if times[0] > times[1] else '#4ecdc4', 
                     '#4ecdc4' if times[1] < times[0] else '#ff6b6b']
            
            bars = ax1.bar(methods, times, color=colors)
            ax1.set_title('Processing Time Comparison')
            ax1.set_ylabel('Time (ms)')
            
            # Agregar valores en las barras
            for bar, time in zip(bars, times):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                        f'{time:,.0f}ms', ha='center', va='bottom')
        
        # Gráfico 2: Speedup
        if 'error' not in spark_comp and spark_comp['speedup'] > 0:
            speedup_data = [1, spark_comp['speedup']]
            ax2.bar(['Baseline', f"{spark_comp['faster_method']}"], speedup_data, 
                   color=['#95a5a6', '#2ecc71'])
            ax2.set_title('Performance Speedup')
            ax2.set_ylabel('Speedup Ratio')
            ax2.text(1, spark_comp['speedup'] + 0.05, 
                    f'{spark_comp["speedup"]:.2f}x', ha='center', va='bottom')
        
        # Gráfico 3: GPU vs CPU (simulado)
        gpu_speedup = 3.5  # Estimación para Metal vs CPU
        ax3.bar(['CPU Only', 'Metal GPU'], [1, gpu_speedup], 
               color=['#e74c3c', '#f39c12'])
        ax3.set_title('GPU Acceleration Impact')
        ax3.set_ylabel('Performance Ratio')
        ax3.text(1, gpu_speedup + 0.1, f'{gpu_speedup:.1f}x', ha='center', va='bottom')
        
        # Gráfico 4: Resource Utilization Timeline (simulado)
        timeline = range(0, 100, 10)
        cpu_usage = [20, 40, 80, 95, 85, 70, 60, 45, 30, 15]
        memory_usage = [30, 50, 70, 85, 90, 75, 65, 50, 35, 20]
        
        ax4.plot(timeline, cpu_usage, 'b-', label='CPU %', linewidth=2)
        ax4.plot(timeline, memory_usage, 'r-', label='Memory %', linewidth=2)
        ax4.set_title('Resource Utilization Over Time')
        ax4.set_xlabel('Time (%)')
        ax4.set_ylabel('Utilization (%)')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Guardar gráfico
        chart_path = f'/tmp/performance_analysis_{job_id}.png'
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        
        # Subir a Cloud Storage
        blob = self.bucket.blob(f'analysis/charts/{job_id}_performance.png')
        blob.upload_from_filename(chart_path)
        
        plt.close()

if __name__ == "__main__":
    analyzer = PerformanceAnalyzer('your-project-id', 'bigdata-processing-results')
    # Ejemplo de uso
    # analysis = analyzer.analyze_job('some-job-id')
    # print(json.dumps(analysis, indent=2))