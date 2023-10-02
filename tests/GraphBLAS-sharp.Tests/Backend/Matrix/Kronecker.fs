module GraphBLAS.FSharp.Tests.Backend.Matrix.Kronecker

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.TestCases
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Backend.Quotes

let config =
    { Utils.defaultConfig with
          endSize = 100
          maxTest = 20 }

let logger = Log.create "kronecker.Tests"

let workGroupSize = Utils.defaultWorkGroupSize

let makeTest testContext zero isEqual op kroneckerFun (leftMatrix: 'a [,], rightMatrix: 'a [,]) =
    let context = testContext.ClContext
    let processor = testContext.Queue

    let m1 =
        Utils.createMatrixFromArray2D CSR leftMatrix (isEqual zero)

    let m2 =
        Utils.createMatrixFromArray2D CSR rightMatrix (isEqual zero)

    let expected =
        HostPrimitives.array2DKroneckerProduct leftMatrix rightMatrix op

    let expected =
        Utils.createMatrixFromArray2D COO expected (isEqual zero)

    let expectedOption =
        if expected.NNZ = 0 then
            None
        else
            expected |> Some

    if m1.NNZ > 0 && m2.NNZ > 0 then
        let m1 = m1.ToDevice context
        let m2 = m2.ToDevice context

        let result =
            kroneckerFun processor ClContextExtensions.HostInterop m1 m2

        let actual =
            Option.map (fun (m: ClMatrix<'a>) -> m.ToHost processor) result

        m1.Dispose processor
        m2.Dispose processor

        match result with
        | Some m -> m.Dispose processor
        | _ -> ()

        // Check result
        "Matrices should be equal"
        |> Expect.equal actual expectedOption

let createGeneralTest testContext (zero: 'a) isEqual op opQ testName =
    Operations.kronecker opQ testContext.ClContext workGroupSize
    |> makeTest testContext zero isEqual op
    |> testPropertyWithConfig config $"test on %A{typeof<'a>} %s{testName}"

let generalTests (testContext: TestContext) =
    [ testContext.Queue.Error.Add(fun e -> failwithf "%A" e)

      createGeneralTest testContext false (=) (&&) ArithmeticOperations.boolMulOption "mul"
      createGeneralTest testContext false (=) (||) ArithmeticOperations.boolSumOption "sum"

      createGeneralTest testContext 0 (=) (*) ArithmeticOperations.intMulOption "mul"
      createGeneralTest testContext 0 (=) (+) ArithmeticOperations.intSumOption "sum"

      createGeneralTest testContext 0uy (=) (*) ArithmeticOperations.byteMulOption "mul"
      createGeneralTest testContext 0uy (=) (+) ArithmeticOperations.byteSumOption "sum"

      createGeneralTest testContext 0.0f Utils.float32IsEqual (*) ArithmeticOperations.float32MulOption "mul"
      createGeneralTest testContext 0.0f Utils.float32IsEqual (+) ArithmeticOperations.float32SumOption "sum"

      if Utils.isFloat64Available testContext.ClContext.ClDevice then
          createGeneralTest testContext 0.0 Utils.floatIsEqual (*) ArithmeticOperations.floatMulOption "mul"
          createGeneralTest testContext 0.0 Utils.floatIsEqual (+) ArithmeticOperations.floatSumOption "sum" ]

let tests =
    gpuTests "Backend.Matrix.kronecker tests" generalTests
