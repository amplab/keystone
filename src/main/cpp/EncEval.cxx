 /** @internal
 ** @file     FisherExtractor.cxx
 ** @brief    JNI Wrapper for enceval GMM and Fisher Vector
 **/

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <ctype.h>
#include <string.h>
#include <iostream>
#include <fstream>

#include <gmm.h>
#include <fisher.h>   
   
#include "EncEval.h"

JNIEXPORT jfloatArray JNICALL Java_keystoneml_utils_external_EncEval_calcAndGetFVs (
  	JNIEnv* env, 
  	jobject obj,
  	jfloatArray means,
  	jint n_dim,
  	jint n_gauss, 
  	jfloatArray covariances,
  	jfloatArray priors,
  	jfloatArray dsiftdescriptors) {

  jsize means_length = env->GetArrayLength(means);   
  jfloat* means_body = env->GetFloatArrayElements(means, 0);
  jsize covar_length = env->GetArrayLength(covariances);   
  jfloat* covar_body = env->GetFloatArrayElements(covariances, 0);
  jsize prior_length = env->GetArrayLength(priors);   
  jfloat* prior_body = env->GetFloatArrayElements(priors, 0);
  jsize descs_length = env->GetArrayLength(dsiftdescriptors)/n_dim;   
  jfloat* descs_body = env->GetFloatArrayElements(dsiftdescriptors, 0);

  float * fk = 0;
  // malloc the result 
  jsize fvenc_length = 2 * n_dim * n_gauss;
  fk = (float*) malloc( sizeof(float) * fvenc_length);

  if ( fk == NULL ) {
    printf("Error allocating memory for the FVenc buffer\n");
    fflush(stdout);
    exit(-1);
  }
  
  printf("input to vectors\n");
  fflush(stdout);
  std::vector<float*> mean(n_gauss);
  std::vector<float*> variance(n_gauss);
  for (int j = 0; j < n_gauss; ++j) {
    mean[j] = &means_body[j*n_dim];
    variance[j] = &covar_body[j*n_dim];
  }
  std::vector<float> coef(prior_body, prior_body + n_gauss);

  // prepare a GMM model with data from the structure
  gaussian_mixture<float> gmmproc(n_gauss,n_dim);
  printf("make gmm\n");
  fflush(stdout);
  gmmproc.set(mean, variance, coef);  

  // construct a c++ struct with default parameter values
  // in the Mex the settings are sent as a third variable, we stick to defaults.
  fisher_param fisher_encoder_params;
  fisher_encoder_params.alpha = 1.0f;
  fisher_encoder_params.pnorm = 0.0f;
  
  fisher_encoder_params.print();

  // What is the role of this fhisher_handle.. can we perhaps keep it between calls.. (save time)?
  printf("make handle \n");
  fflush(stdout);
  fisher<float> fisher_encoder(fisher_encoder_params);
  // initialise encoder with a GMM model (vocabulary)
  printf(".. and set gmm model to handle \n");
  fflush(stdout);
  fisher_encoder.set_model(gmmproc);

  printf("descriptors to vector \n");
  printf("descriptors length: %d\n", descs_length);
  fflush(stdout);
  std::vector<float*> x(descs_length);

  for (int j = 0; j < descs_length; ++j) {
    x[j] = &descs_body[j*n_dim];
  }

  bool weights = false;
  // load in weights if specified
  // do encoding

  printf("encode without weights \n");
  fflush(stdout);
  fisher_encoder.compute(x, fk);

  jfloatArray result = env->NewFloatArray(fvenc_length);
  if (result == NULL) {
    printf("Error geting memory in the JNI for the result fisher vector\n");
    fflush(stdout);
    return NULL;
  }

  // get a pointer to the new array
  printf("Copy to JNI return memory\n");
  fflush(stdout);
  env->SetFloatArrayRegion(result, 0, fvenc_length, fk);
  printf("Calling free on fvenc\n");
  fflush(stdout);

  env->ReleaseFloatArrayElements(means, means_body, 0);
  env->ReleaseFloatArrayElements(covariances, covar_body, 0);
  env->ReleaseFloatArrayElements(priors, prior_body, 0);
  env->ReleaseFloatArrayElements(dsiftdescriptors, descs_body, 0);
  free(fk);
  return result;
}


JNIEXPORT jfloatArray JNICALL Java_keystoneml_utils_external_EncEval_computeGMM (
  	JNIEnv * env, 
  	jobject obj, 
  	jint n_gauss, 
  	jint n_dim, 
  	jfloatArray gmm_samples) {
		
  //For now this returns everything as one big fat array. This is absolutely disgusting.
  
  //Get samples from Java.
  jsize n_samples = env->GetArrayLength(gmm_samples)/n_dim;   
  jfloat* samples_body = env->GetFloatArrayElements(gmm_samples, 0);
  
  //Copy to C vectors. We assume things come at us sample at a time.
  std::vector<float*> samples(n_samples);
  for (int i = 0; i < n_samples; ++i) {
    samples[i] = &samples_body[i*n_dim];
  }
  
  //Create a default empty gmm parameter set.
  em_param gmm_params; 
  gaussian_mixture<float> gmmproc(gmm_params, n_gauss, n_dim);
  
  //We don't yet accept initial means/variances/coefs yet.
  int seed = 42;
  //int seed = time(NULL);
  gmmproc.random_init(samples, seed);
  
  //Run EM.
  gmmproc.em(samples);
  
  //Copy final stuff back out.
  int meanResSize = n_gauss*n_dim;
  int varResSize = n_gauss*n_dim;
  int coefResSize = n_gauss;
  int totalResSize = meanResSize + varResSize + coefResSize;
  
  //Allocate output arrays.
  float* meanRes = (float *) malloc(meanResSize*sizeof (float)); //array of size(ndim,ngauss)
  float* varRes = (float *) malloc(varResSize*sizeof (float)); //array of size(ndim,ngauss)
  float* coefRes = (float *) malloc(coefResSize*sizeof (float)); //array of size(ndim)
  
  for (int j = 0; j < n_gauss; ++j) {
    float* componentmean = gmmproc.get_mean(j);
    float* componentvariance = gmmproc.get_variance(j);

    for (int i = 0; i < n_dim; ++i) {
      meanRes[i+j*n_dim] = componentmean[i];
      varRes[i+j*n_dim] = componentvariance[i];
    }

    coefRes[j] = gmmproc.get_mixing_coefficients(j);
  }
  
  //Copy the results back to Java land.
  jfloatArray result = env->NewFloatArray(totalResSize);
  env->SetFloatArrayRegion(result, 0, meanResSize, meanRes);
  env->SetFloatArrayRegion(result, meanResSize, varResSize, varRes);
  env->SetFloatArrayRegion(result, meanResSize+varResSize, coefResSize, coefRes);

  env->ReleaseFloatArrayElements(gmm_samples, samples_body, 0);
  //Cleanup structs created;
  free(meanRes);
  meanRes = NULL;

  free(varRes);
  varRes = NULL;

  free(coefRes);
  coefRes = NULL;

  return result; 
}
