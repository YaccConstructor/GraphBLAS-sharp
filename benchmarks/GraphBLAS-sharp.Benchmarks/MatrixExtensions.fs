namespace GraphBLAS.FSharp.Benchmarks

open GraphBLAS.FSharp.Objects
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects.ClMatrix

module MatrixExtensions =
    type Matrix<'a when 'a : struct> with
        static member ToBackendCOO (context: ClContext) matrix =
            match matrix with
            | Matrix.COO m ->
                let rows =
                    context.CreateClArray(
                        m.Rows,
                        hostAccessMode = HostAccessMode.ReadOnly,
                        deviceAccessMode = DeviceAccessMode.ReadOnly,
                        allocationMode = AllocationMode.CopyHostPtr
                    )

                let cols =
                    context.CreateClArray(
                        m.Columns,
                        hostAccessMode = HostAccessMode.ReadOnly,
                        deviceAccessMode = DeviceAccessMode.ReadOnly,
                        allocationMode = AllocationMode.CopyHostPtr
                    )

                let vals =
                    context.CreateClArray(
                        m.Values,
                        hostAccessMode = HostAccessMode.ReadOnly,
                        deviceAccessMode = DeviceAccessMode.ReadOnly,
                        allocationMode = AllocationMode.CopyHostPtr
                    )

                { Context = context
                  RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  Rows = rows
                  Columns = cols
                  Values = vals }

            | _ -> failwith "Unsupported matrix format: %A"

        static member ToBackendCSR (context: ClContext) matrix =
            let rowIndices2rowPointers (rowIndices: int []) rowCount =
                let nnzPerRow = Array.zeroCreate rowCount
                let rowPointers = Array.zeroCreate rowCount

                Array.iter (fun rowIndex -> nnzPerRow.[rowIndex] <- nnzPerRow.[rowIndex] + 1) rowIndices

                for i in 1 .. rowCount - 1 do
                    rowPointers.[i] <- rowPointers.[i - 1] + nnzPerRow.[i - 1]

                rowPointers

            match matrix with
            | Matrix.COO m ->
                let rowPointers =
                    context.CreateClArray(
                        rowIndices2rowPointers m.Rows m.RowCount,
                        hostAccessMode = HostAccessMode.ReadOnly,
                        deviceAccessMode = DeviceAccessMode.ReadOnly,
                        allocationMode = AllocationMode.CopyHostPtr
                    )

                let cols =
                    context.CreateClArray(
                        m.Columns,
                        hostAccessMode = HostAccessMode.ReadOnly,
                        deviceAccessMode = DeviceAccessMode.ReadOnly,
                        allocationMode = AllocationMode.CopyHostPtr
                    )

                let vals =
                    context.CreateClArray(
                        m.Values,
                        hostAccessMode = HostAccessMode.ReadOnly,
                        deviceAccessMode = DeviceAccessMode.ReadOnly,
                        allocationMode = AllocationMode.CopyHostPtr
                    )

                { Context = context
                  RowCount = m.RowCount
                  ColumnCount = m.ColumnCount
                  RowPointers = rowPointers
                  Columns = cols
                  Values = vals }

            | _ -> failwith "Unsupported matrix format: %A"

