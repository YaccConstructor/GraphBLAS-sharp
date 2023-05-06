module GraphBLAS.FSharp.Tests.Backend.Matrix.Kronecker.ByRows

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.TestCases
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions

let config =
    { Utils.defaultConfig with
          endSize = 50
          maxTest = 30 }

let logger = Log.create "kronecker.Tests"

let workGroupSize = Utils.defaultWorkGroupSize

let makeTest context processor zero isEqual op kroneckerFun (leftMatrix: 'a [,], rightMatrix: 'a [,]) =
    let m1 =
        Utils.createMatrixFromArray2D CSR leftMatrix (isEqual zero)

    let m2 =
        Utils.createMatrixFromArray2D CSR rightMatrix (isEqual zero)

    let expected =
        HostPrimitives.array2DKroneckerProduct leftMatrix rightMatrix op

    let expected =
        Matrix.LIL.FromArray2D(expected, isEqual zero)

    if m1.NNZ > 0 && m2.NNZ > 0 then
        let m1 = m1.ToDevice context
        let m2 = m2.ToDevice context

        let actual: ClMatrix<_> =
            kroneckerFun processor ClContext.HostInterop m1 m2

        m1.Dispose processor
        m2.Dispose processor

        let actual = actual.ToHostAndFree processor

        match actual with
        | Matrix.LIL actual -> Utils.compareLILMatrix isEqual actual expected
        | _ -> failwith "Matrix format are not matching"

let createGeneralTest (context: ClContext) (processor: MailboxProcessor<Msg>) (zero: 'a) isEqual op opQ testName =

    let kronecker =
        Matrix.kroneckerByRows opQ context workGroupSize

    makeTest context processor zero isEqual op kronecker
    |> testPropertyWithConfig config $"test on %A{typeof<'a>} %s{testName}"

let generalTests (testContext: TestContext) =
    [ let context = testContext.ClContext
      let queue = testContext.Queue
      queue.Error.Add(fun e -> failwithf "%A" e)

      createGeneralTest context queue false (=) (&&) ArithmeticOperations.boolMulOption "mul"
      createGeneralTest context queue false (=) (||) ArithmeticOperations.boolSumOption "sum"

      createGeneralTest context queue 0 (=) (*) ArithmeticOperations.intMulOption "mul"
      createGeneralTest context queue 0 (=) (+) ArithmeticOperations.intSumOption "sum"

      createGeneralTest context queue 0uy (=) (*) ArithmeticOperations.byteMulOption "mul"
      createGeneralTest context queue 0uy (=) (+) ArithmeticOperations.byteSumOption "sum"

      createGeneralTest context queue 0.0f Utils.float32IsEqual (*) ArithmeticOperations.float32MulOption "mul"
      createGeneralTest context queue 0.0f Utils.float32IsEqual (+) ArithmeticOperations.float32SumOption "sum"

      if Utils.isFloat64Available context.ClDevice then
          createGeneralTest context queue 0.0 Utils.floatIsEqual (*) ArithmeticOperations.floatMulOption "mul"
          createGeneralTest context queue 0.0 Utils.floatIsEqual (+) ArithmeticOperations.floatSumOption "sum" ]

let tests =
    gpuTests "Backend.Matrix.kronecker tests" generalTests
