// metal_preprocessing.metal
#include <metal_stdlib>
using namespace metal;

kernel void normalize_array(device float* input_array [[buffer(0)]],
                           device float* output_array [[buffer(1)]],
                           constant float& mean [[buffer(2)]],
                           constant float& std_dev [[buffer(3)]],
                           uint index [[thread_position_in_grid]]) {
    output_array[index] = (input_array[index] - mean) / std_dev;
}

kernel void matrix_multiply(device float* matrix_a [[buffer(0)]],
                           device float* matrix_b [[buffer(1)]],
                           device float* result [[buffer(2)]],
                           constant uint& width [[buffer(3)]],
                           uint2 position [[thread_position_in_grid]]) {
    uint row = position.y;
    uint col = position.x;
    uint index = row * width + col;
    
    float sum = 0.0;
    for (uint k = 0; k < width; k++) {
        sum += matrix_a[row * width + k] * matrix_b[k * width + col];
    }
    result[index] = sum;
}