#include "udxinc.h"
#include <unistd.h>

using namespace nz::udx_ver2;

class CMyProduct: public nz::udx_ver2::Udf
{
public:
    CMyProduct(UdxInit *pInit) : Udf(pInit)
    {
    }
    static nz::udx_ver2::Udf* instantiate(UdxInit *pInit);
    virtual nz::udx_ver2::ReturnValue evaluate()
    {
        int int1= int32Arg(0);
        int int2= int32Arg(1);
        int retVal = int1 * int2;

        NZ_UDX_RETURN_INT32(retVal);
    }
};
nz::udx_ver2::Udf* CMyProduct::instantiate(UdxInit *pInit)
{
    return new CMyProduct(pInit);
}
