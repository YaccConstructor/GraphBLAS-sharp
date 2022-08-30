namespace GraphBLAS.FSharp.Backend.COOVector

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.COOVector.Utilities
open GraphBLAS.FSharp.Backend.COOVector.Utilities.EWiseAdd

module internal EWiseAdd =
    let private runNonEmpty
        (leftIndices: int [])
        (leftValues: 'a [])
        (rightIndices: int [])
        (rightValues: 'a [])
        (mask: Mask1D option)
        (semiring: ISemiring<'a>)
        : ClTask<int [] * 'a []> =

        opencl {
            let! allIndices, allValues = merge leftIndices leftValues rightIndices rightValues mask

            let (ClosedBinaryOp plus) = semiring.Plus
            let! rawPositions = preparePositions allIndices allValues plus

            return! setPositions allIndices allValues rawPositions
        }

    let run
        (leftIndices: int [])
        (leftValues: 'a [])
        (rightIndices: int [])
        (rightValues: 'a [])
        (mask: Mask1D option)
        (semiring: ISemiring<'a>)
        : ClTask<int [] * 'a []> =

        if leftValues.Length = 0 then
            opencl {
                let! resultIndices = Copy.copyArray rightIndices
                let! resultValues = Copy.copyArray rightValues

                return resultIndices, resultValues
            }
        elif rightIndices.Length = 0 then
            opencl {
                let! resultIndices = Copy.copyArray leftIndices
                let! resultValues = Copy.copyArray leftValues

                return resultIndices, resultValues
            }
        else
            runNonEmpty leftIndices leftValues rightIndices rightValues mask semiring
