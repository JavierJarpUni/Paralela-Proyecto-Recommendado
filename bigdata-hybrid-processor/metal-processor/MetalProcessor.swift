// MetalProcessor.swift
import Foundation
import Metal
import MetalPerformanceShaders

class MetalArrayProcessor {
    private let device: MTLDevice
    private let commandQueue: MTLCommandQueue
    private let library: MTLLibrary
    
    init() throws {
        guard let device = MTLCreateSystemDefaultDevice() else {
            throw ProcessingError.deviceNotFound
        }
        self.device = device
        
        guard let commandQueue = device.makeCommandQueue() else {
            throw ProcessingError.commandQueueCreationFailed
        }
        self.commandQueue = commandQueue
        
        guard let library = device.makeDefaultLibrary() else {
            throw ProcessingError.libraryNotFound
        }
        self.library = library
    }
    
    func normalizeArray(_ inputArray: [Float]) throws -> [Float] {
        let mean = inputArray.reduce(0, +) / Float(inputArray.count)
        let variance = inputArray.map { pow($0 - mean, 2) }.reduce(0, +) / Float(inputArray.count)
        let stdDev = sqrt(variance)
        
        return try processWithMetal(inputArray, mean: mean, stdDev: stdDev)
    }
    
    private func processWithMetal(_ inputArray: [Float], mean: Float, stdDev: Float) throws -> [Float] {
        guard let function = library.makeFunction(name: "normalize_array") else {
            throw ProcessingError.functionNotFound
        }
        
        let pipelineState = try device.makeComputePipelineState(function: function)
        
        let inputBuffer = device.makeBuffer(bytes: inputArray, 
                                          length: inputArray.count * MemoryLayout<Float>.size,
                                          options: [])
        
        let outputBuffer = device.makeBuffer(length: inputArray.count * MemoryLayout<Float>.size,
                                           options: [])
        
        let meanBuffer = device.makeBuffer(bytes: [mean],
                                         length: MemoryLayout<Float>.size,
                                         options: [])
        
        let stdDevBuffer = device.makeBuffer(bytes: [stdDev],
                                           length: MemoryLayout<Float>.size,
                                           options: [])
        
        let commandBuffer = commandQueue.makeCommandBuffer()!
        let encoder = commandBuffer.makeComputeCommandEncoder()!
        
        encoder.setComputePipelineState(pipelineState)
        encoder.setBuffer(inputBuffer, offset: 0, index: 0)
        encoder.setBuffer(outputBuffer, offset: 0, index: 1)
        encoder.setBuffer(meanBuffer, offset: 0, index: 2)
        encoder.setBuffer(stdDevBuffer, offset: 0, index: 3)
        
        let threadsPerGroup = MTLSize(width: 256, height: 1, depth: 1)
        let numThreadgroups = MTLSize(width: (inputArray.count + 255) / 256, height: 1, depth: 1)
        
        encoder.dispatchThreadgroups(numThreadgroups, threadsPerThreadgroup: threadsPerGroup)
        encoder.endEncoding()
        
        commandBuffer.commit()
        commandBuffer.waitUntilCompleted()
        
        let resultPointer = outputBuffer!.contents().bindMemory(to: Float.self, capacity: inputArray.count)
        return Array(UnsafeBufferPointer(start: resultPointer, count: inputArray.count))
    }
}

enum ProcessingError: Error {
    case deviceNotFound
    case commandQueueCreationFailed
    case libraryNotFound
    case functionNotFound
}