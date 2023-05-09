module GraphBLAS.FSharp.Tests.Backend.Matrix.Kronecker

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
          endSize = 30
          maxTest = 20 }

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
            kroneckerFun processor ClContext.HostInterop m1 m2

        let actual =
            Option.bind (fun (m: ClMatrix<'a>) -> m.ToHost processor |> Some) result

        m1.Dispose processor
        m2.Dispose processor

        match result with
        | Some m -> m.Dispose processor
        | _ -> ()

        // Check result
        "Matrices should be equal"
        |> Expect.equal actual expectedOption

let createGeneralTest (context: ClContext) (processor: MailboxProcessor<Msg>) (zero: 'a) isEqual op opQ testName =

    let kronecker =
        Matrix.kronecker opQ context workGroupSize

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
