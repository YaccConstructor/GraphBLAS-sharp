module GraphBLAS.FSharp.Tests.Backend.Matrix.SpGeMM.ExpandCOO

open Expecto
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Backend

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let config =
    { Utils.defaultConfig with
          arbitrary = [ typeof<Generators.PairOfSparseMatricesWithCompatibleSizes> ] }

let makeGeneralTest zero isEqual opAdd opMul testFun (leftArray: 'a [,], rightArray: 'a [,]) =

    let leftMatrix =
        Utils.createMatrixFromArray2D COO leftArray (isEqual zero)

    let rightMatrix =
        Utils.createMatrixFromArray2D CSR rightArray (isEqual zero)

    if leftMatrix.NNZ > 0 && rightMatrix.NNZ > 0 then
        let clLeftMatrix = leftMatrix.ToDevice context
        let clRightMatrix = rightMatrix.ToDevice context

        let (clMatrixActual: ClMatrix.COO<_> option) =
            testFun processor HostInterop clLeftMatrix clRightMatrix

        let expected =
            HostPrimitives.array2DMultiplication zero opMul opAdd leftArray rightArray
            |> fun array -> Matrix.COO.FromArray2D(array, isEqual zero)

        match clMatrixActual with
        | Some clMatrixActual ->

            let matrixActual = clMatrixActual.ToHost processor
            clMatrixActual.Dispose processor

            Utils.compareCOOMatrix isEqual matrixActual expected
        | None ->
            "Expected should be empty"
            |> Expect.isTrue (expected.NNZ = 0)

let createGeneralTest (zero: 'a) isEqual (opAddQ, opAdd) (opMulQ, opMul) testFun =
    testFun opAddQ opMulQ context Utils.defaultWorkGroupSize
    |> makeGeneralTest zero isEqual opAdd opMul
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let generalTests =
    [ createGeneralTest 0 (=) ArithmeticOperations.intAdd ArithmeticOperations.intMul Operations.SpGeMM.COO.expand

      if Utils.isFloat64Available context.ClDevice then
          createGeneralTest
              0.0
              Utils.floatIsEqual
              ArithmeticOperations.floatAdd
              ArithmeticOperations.floatMul
              Operations.SpGeMM.COO.expand

      createGeneralTest
          0.0f
          Utils.float32IsEqual
          ArithmeticOperations.float32Add
          ArithmeticOperations.float32Mul
          Operations.SpGeMM.COO.expand
      createGeneralTest false (=) ArithmeticOperations.boolAdd ArithmeticOperations.boolMul Operations.SpGeMM.COO.expand ]
    |> testList "General"

let tests =
    testList "SpGeMM.Expand" [ generalTests ]
