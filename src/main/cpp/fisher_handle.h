
/// \Class    fisher_handle fisher_handle.h "fisher_handle.h"
///  
/// \brief
///
/// \version  1.0
/// \author   Ken Chatfield
/// \date     08/07/2011

#ifndef __FISHER_HANDLE_H
#define __FISHER_HANDLE_H

#include <fisher.h>
#include <gmm.h>
#include <stdint.h>

#define CLASS_HANDLE_SIGNATURE 0xa5a50f0f
template<class T> class fisher_handle: public fisher<T>
{
public:
    fisher_handle(gaussian_mixture<T> &gmm, fisher_param params): fisher<T>(params) { signature = CLASS_HANDLE_SIGNATURE; gmmproc = &gmm; }
    ~fisher_handle() { signature = 0; }
    bool isValid() { return (signature == CLASS_HANDLE_SIGNATURE); }
    gaussian_mixture<T>* getGmmPtr() { return gmmproc; }
private:
    uint32_t signature;
    gaussian_mixture<T> *gmmproc;
};

#endif
