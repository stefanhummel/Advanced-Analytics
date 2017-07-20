#include "udxinc.h"
using namespace nz::udx_ver2;

class CPenMax : public nz::udx_ver2::Uda
{
public:
    CPenMax(UdxInit *pInit) : Uda(pInit) {}
    static nz::udx_ver2::Uda* instantiate(UdxInit *pInit);
    virtual void initializeState();
    virtual void accumulate();
    virtual void merge();
    virtual ReturnValue finalResult();
};

nz::udx_ver2::Uda* CPenMax::instantiate(UdxInit *pInit)
{
    return new CPenMax(pInit);
}

void CPenMax::initializeState()
{
    setStateNull(0, true); // set current max to null
    setStateNull(1, true); // set current penmax to null
}

void CPenMax::accumulate()
{
    int *pCurMax = int32State(0);
    bool curMaxNull = isStateNull(0);
    int *pCurPenMax = int32State(1);
    bool curPenMaxNull = isStateNull(1);
    int curVal = int32Arg(0);
    bool curValNull = isArgNull(0);

    if ( !curValNull ) { // do nothing if argument is null - can't
                         //affect max or penmax
        if ( curMaxNull ) { // if current max is null, this arg
                            //becomes current max
            setStateNull(0, false); // current max no longer null
            *pCurMax = curVal;
        } else 
            { if ( curVal > *pCurMax ) { // if arg is new max
                setStateNull(1, false); // then prior current max
                                       // becomes current penmax
                *pCurPenMax = *pCurMax;
                *pCurMax = curVal; // and current max gets arg
            } else if ( curPenMaxNull || curVal > *pCurPenMax ){
                        // arg might be greater than current penmax
                setStateNull(1, false); // it is
                *pCurPenMax = curVal;
            }
        }
    }
}

void CPenMax::merge()
{
    int *pCurMax = int32State(0);
    bool curMaxNull = isStateNull(0);
    int *pCurPenMax = int32State(1);
    bool curPenMaxNull = isStateNull(1);
    int nextMax = int32Arg(0);
    bool nextMaxNull = isArgNull(0);
    int nextPenMax = int32Arg(1);
    bool nextPenMaxNull = isArgNull(1);

    if ( !nextMaxNull ) { // if next max is null, then so is 
                          //next penmax and we do nothing
        if ( curMaxNull ) {
            setStateNull(0, false); // current max was null, 
                                   // so save next max
            *pCurMax = nextMax;
        } else {
           if ( nextMax > *pCurMax ) {
               setStateNull(1, false);
                  // next max is greater than current, so save next
               *pCurPenMax = *pCurMax;
                  // and make current penmax prior current max
               *pCurMax = nextMax;
           } else if ( curPenMaxNull || nextMax > *pCurPenMax ) {
                // next max may be greater than current penmax
               setStateNull(1, false); // it is
               *pCurPenMax = nextMax;
           }
        }

        if ( !nextPenMaxNull ) {
            if ( isStateNull(1) ) {
              // can't rely on curPenMaxNull here, might have
              // change state var null flag above
                setStateNull(1, false); // first non-null penmax,
                                        // save it
                *pCurPenMax = nextPenMax;
            } else {
                if ( nextPenMax > *pCurPenMax ) {
                    *pCurPenMax = nextPenMax;
                    // next penmax greater than current, save it
                }
            }
        }
    }
}

ReturnValue CPenMax::finalResult()
{
    int curPenMax = int32Arg(1);
    bool curPenMaxNull = isArgNull(1);

    if ( curPenMaxNull )
        NZ_UDX_RETURN_NULL();
    setReturnNull(false);
    /*    NZ_UDX_RETURN_INT32(curPenMax);*/
    NZ_UDX_RETURN_INT32(3);
}
