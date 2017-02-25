 /** @internal
 ** @file     SIFTExtractor.cpp
 ** @brief    Dense Scale Invariant Feature Transform (SIFT) - Driver, with JNI library support. 
 **/

#include <vl/generic.h>
#include <vl/sift.h>
#include <vl/dsift.h>
#include <vl/imopv.h>
#include <vl/mathop.h>
#include <vl/pgm.h>

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include <ctype.h>
#include <string.h>

#include <iostream>
#include <fstream>
   
#include "VLFeat.h"
   
const int dims = 128;

struct DescSet {
  float* descriptors;
  int numDesc;
};

int* gloabalcount = 0;

DescSet* getMultiScaleDSIFTs_f(VlPgmImage* pim, float* imgData, int step, int bin, int numScales, int scaleStep);


DescSet* getMultiScaleDSIFTs_f(
    VlPgmImage* pim, 
    float* imgData, 
    int step,
    int bin, 
    int imgNumScales,
    int scaleStep) {
		
  float magnif = 6.0;
  // the number of descriptors extracted in total, also the value this function returns
  int retVal = 0;
  // container for the filters 
  VlDsiftFilter   **dfilt   = (VlDsiftFilter**) malloc( sizeof(VlDsiftFilter*)*imgNumScales ); 
  vl_bool useFlatWindow = VL_FALSE; 
  double windowSize = -1.0 ;
  // create a buffer for the smoothed images in the "fake" scaling 
  int imgArraySize = vl_pgm_get_npixels (pim) *vl_pgm_get_bpp(pim);

  // set ddata to the original image for the first iteration. 
  float *imgDataScale = (float*) malloc( imgArraySize * sizeof (float) );
  float *ddata = imgDataScale;
  DescSet *retValSet = (DescSet*) malloc(sizeof(DescSet));
  const float **descSet = (const float **) malloc( sizeof(float*)*imgNumScales );
  retValSet->descriptors = NULL;
  retValSet->numDesc = 0;
  float contrastthreshold = 0.005;

  int numDescPerRow = 0;
  int *numDescPerScale = (int*) malloc( sizeof(int)*imgNumScales );

  // Setup the filters, smothe the image, compute the descriptors and count them. 
  for (int scale=0; scale<imgNumScales; ++scale) {
    // set the bin size for this scale
    //       0 = bin; 1 = bin +2; 2 = bin + 4; 3 = bin + 6; 4 = bin + 8;
    int scaleValue = bin + (scale*2);
    // create the filter for this scale
    fflush(stdout);

    // Note, the step + scale*scaleStep term controls the granularity of sampling at each SIFT scale.
    // scaleStep=0 is the setting used by the enceval code out of the box.
    dfilt[scale] = vl_dsift_new_basic(pim->width, pim->height, step + scale*scaleStep, scaleValue);
    if (dfilt[scale] == NULL) {
      printf("Error creating filter at scale %i\n", scale);
      fflush(stdout);  
      exit(1);
    }
    
    // smooth the image appropriatley for this scale. 
    double sigma = (scaleValue / magnif);

    vl_size smoothstride = pim->width;
    vl_size stride = pim->width;

    vl_imsmooth_f(ddata, smoothstride, imgData, pim->width, pim->height, stride, sigma, sigma);

    // we need to set the bounding box such that all the descriptors are calculated from the same x,y coordinates.
    int off =  (1+(imgNumScales*2)) - (scale*3);
    //vl_dsift_set_bounds(dfilt[scale], off, off, (pim->width)-1-off, (pim->height)-1-off);
    vl_dsift_set_bounds(dfilt[scale], off, off, pim->width-1, pim->height-1);
    // useing a flat window = fast-option.. runs much much faster but if set to false it's more equivelant to 
    // the original SIFT algorithm. 
    useFlatWindow = VL_TRUE;
    vl_dsift_set_flat_window(dfilt[scale], useFlatWindow) ;
    // set the window scalara size
    double windowSize = 1.5 ;
    vl_dsift_set_window_size(dfilt[scale], windowSize) ;

    // For the dens-sift we just call the vl_dsift_process function. 
    vl_dsift_process(dfilt[scale], ddata);
    // sum up the number of descriptors created. 
    numDescPerScale[scale] = vl_dsift_get_keypoint_num( dfilt[scale] );
    retValSet->numDesc += numDescPerScale[scale];

    // get the keypoints for the descriptors
    VlDsiftKeypoint const *dkeys = vl_dsift_get_keypoints( dfilt[scale] );

    // get the descriptors for this scale 
    descSet[scale] = vl_dsift_get_descriptors( dfilt[scale] );

    if ( descSet[scale] == NULL) {
      printf("\n\tError getting descriptor data \n");
      fflush(stdout);  
      exit(-1);
    }
    // set the working pointer to he scaled buffer (i.e. the one used after the first iteration)
    ddata = imgDataScale;
  }
  // create the Memory for the results
  retValSet->descriptors = (float*) malloc(retValSet->numDesc*dims*sizeof(float));
  int currRes = 0;
  if (retValSet->descriptors == NULL) {
    printf("\nError in allocating memory for return values retValSet->descriptors\n");
    fflush(stdout);  
  }
  // collecting the results grouped by pixel value for each scale 
  int globalLoc = 0;
  int localoffset = 0;
  bool groupByPixels = false;

  if (groupByPixels) {
    // group all desc of same (x,y) for each scale 
    // is done by default if the number of descriptors for each scale is the same
    for (int i=0; i<numDescPerScale[0]; i++) {  
      for (int scale=0; scale<imgNumScales; scale++) {
        // if the descriptor norm is below the threshold we zero it out 
        bool copy = true;
        if ( vl_dsift_get_keypoints( dfilt[scale])[i].norm < contrastthreshold ) {
          copy = false;
        }
        for (int x=0; x<dims; x++) {
          if (copy) {
            retValSet->descriptors[globalLoc++] = descSet[scale][localoffset++];
          }else {
            retValSet->descriptors[globalLoc++] = 0;
            localoffset++;
          }
        } 
      } // end for scale
      localoffset += 128;
    } // end for i 
  } else {
    // concatinate desc. of each scale, one after the other, No groupping based on (x,y) coordinates
    // this is done when the number of desriptors for each scale is not the same. 
    for (int scale=0; scale<imgNumScales; scale++) {
      VlDsiftKeypoint const *dkeys = vl_dsift_get_keypoints( dfilt[scale] );
      localoffset = 0;
      for (int i=0; i<numDescPerScale[scale]; i++) {  
        //printf("point %i, (%f,%f,%f,%f) \n", i, dkeys[i].x, dkeys[i].y, dkeys[i].s, dkeys[i].norm);
        // if the descriptor norm is below the threshold we zero it out 
        bool copy = true;
        if( dkeys[i].norm < contrastthreshold ) {
          copy = false;
        } 
        for (int x=0; x<dims; x++) {
          if (copy) {
            retValSet->descriptors[globalLoc++] = descSet[scale][localoffset++];
          }else {
            retValSet->descriptors[globalLoc++] = 0;
            localoffset++;
          }
        } // x 
      }// i 
      fflush(stdout);
    }// scale
  }

  if ( imgDataScale != 0) {
    free( imgDataScale );
    imgDataScale = 0;
  }
  // free up the filters we made for each scale 
  if (dfilt != 0) {
    for(int scale=0; scale<imgNumScales; ++scale) {
      if (dfilt[scale] != 0) {
        vl_dsift_delete (dfilt[scale]);
        dfilt[scale] = 0;
      }
    }
    free(dfilt);
    dfilt = 0;
  }
  free(descSet);

  return retValSet;
}

JNIEXPORT jshortArray JNICALL Java_keystoneml_utils_external_VLFeat_getSIFTs (
    JNIEnv* env, 
    jobject obj, 
    jint width,
    jint height,
    jint step,
    jint bin,
    jint numScales,
    jint scaleStep,
    jfloatArray image) {
	  
  // Create and set PGMImage metadata
  VlPgmImage pim ; // image info 
  pim.width = width; pim.height = height; pim.max_value = 0; pim.is_raw = 1;
  // Allocate memory for the image actual data 
  int imMemSize = vl_pgm_get_npixels (&pim) *vl_pgm_get_bpp(&pim) * sizeof (float);
  float* pimData = (float*) malloc( imMemSize ) ;
  // Get the access to the Image data from the JNI interface. 
  jsize len = env->GetArrayLength(image);
  jfloat* body = env->GetFloatArrayElements(image, 0);
  jfloat* body_orig = body;
  // extract the Data from the JNIArray and find the pixel-maxvalue 
  if ( pimData != 0 ) {
    for (int i=0; i<vl_pgm_get_npixels (&pim); ++i) {
      // find the correct maximum value of the grayscale image.
      if (*body > pim.max_value) {
        pim.max_value = *body;
      }
      // cast the jint value to float 
      pimData[i] = (*body);
      body++;
    }
    fflush(stdout);
  } else {
    printf ("Error assigning memory for image buffer");
    exit(1);
  }
  // calculate and get the denseSIFTdescriptors 
  // NOTE! we need to clean up this array with free the passed container pointer. 
  DescSet* dSiftSet = getMultiScaleDSIFTs_f(&pim, pimData, step, bin, numScales, scaleStep);
  int numDesc = dSiftSet->numDesc;
  float* floatResult = dSiftSet->descriptors;
  free (dSiftSet);
  jshort* jshortResult = (jshort*) malloc(numDesc*dims*sizeof(jshort));

  // transpose the descriptors. 
  int binT = 8;
  int binX = 4;
  int binY = 4;
  float *tmpDescr = (float*) malloc(sizeof(float) * dims) ;
  if (jshortResult != NULL) {  // loop throug and make unsigned dims and put in short to save memory
    int currLoc = 0;
    for (int i=0; i<numDesc; i++) {
      vl_dsift_transpose_descriptor (tmpDescr, (floatResult + i*128), binT, binX, binY) ;

      for (int x=0; x<dims; x++) {
        
        unsigned int v = (512 * tmpDescr[x]);
        jshortResult[currLoc++] = ((v < 255) ? v : 255);
        
      } // end for x
    } // end for i
  } else {
    printf ("Error in jshortResult array\n");
    fflush(stdout);
    exit(1);
  }
  free(tmpDescr);
  tmpDescr = 0;
  // free the float-result-array we got from getMultiScaleDSIFTs_f
  if(floatResult != NULL) {
    free (floatResult);
  }
  // allocate a JNI array for the descriptors, populate and return.
  jshortArray result = env->NewShortArray((numDesc*dims));
  if (jshortResult != 0) {
    env->SetShortArrayRegion(result, 0, numDesc*dims, jshortResult);
    // free the c++ memory allocated in getMultiScaleDSIFTS_f
    free( jshortResult );
  }
  // free the c++ memory for the image data 
  if ( pimData != 0 ) {
    free( pimData );
    pimData=0;
  }

    env->ReleaseFloatArrayElements(image, body_orig, 0);

  return result;
}
