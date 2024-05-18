#include <cuda_runtime.h>

// 初始化CUDA设备
cudaSetDevice(0); // 设置使用的GPU编号，假设为0


__global__ void searchKnn(float *queryVectors,
						  float *databaseVectors,
						  int *indices,
						  float *distances,
						  int vectorSize,
						  int numVectors,
						  int k) {
  int idx = threadIdx.x + blockDim.x * blockIdx.x;
  if (idx < numVectors) {
	float distance = 0;
	for (int i = 0; i < vectorSize; ++i) {
	  float diff = queryVectors[blockIdx.y * vectorSize + i] - databaseVectors[idx * vectorSize + i];
	  distance += diff * diff;
	}
	distances[idx] = sqrt(distance);
	indices[idx] = idx;
  }
}

__device__ void deviceFunction() {
  float *d_queryVectors, *d_databaseVectors;
  int *d_indices;
  float *d_distances;

  cudaMalloc(&d_queryVectors, sizeof(float) * numQueries * vectorSize);
  cudaMalloc(&d_databaseVectors, sizeof(float) * numVectors * vectorSize);
  cudaMalloc(&d_indices, sizeof(int) * numVectors);
  cudaMalloc(&d_distances, sizeof(float) * numVectors);

  cudaMemcpy(d_queryVectors, host_queryVectors, sizeof(float) * numQueries * vectorSize, cudaMemcpyHostToDevice);
  cudaMemcpy(d_databaseVectors, host_databaseVectors, sizeof(float) * numVectors * vectorSize, cudaMemcpyHostToDevice);

  dim3 blocks(numVectors);
  dim3 threads(1);
  searchKnn<<<blocks, threads>>>(d_queryVectors, d_databaseVectors, d_indices, d_distances, vectorSize, numVectors, k);

// 把结果从GPU内存复制回主机内存
  int *h_indices = new int[numVectors];
  float *h_distances = new float[numVectors];
  cudaMemcpy(h_indices, d_indices, sizeof(int) * numVectors, cudaMemcpyDeviceToHost);
  cudaMemcpy(h_distances, d_distances, sizeof(float) * numVectors, cudaMemcpyDeviceToHost);

// 释放GPU内存
  cudaFree(d_queryVectors);
  cudaFree(d_databaseVectors);
  cudaFree(d_indices);
  cudaFree(d_distances);
}