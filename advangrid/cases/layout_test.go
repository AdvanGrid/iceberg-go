package cases

import (
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func monthPartitioned() catalog.CreateTableOpt {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{3}, FieldID: 1000, Name: "t_month", Transform: iceberg.MonthTransform{},
	})

	return catalog.WithPartitionSpec(&spec)
}

func sortedByK() catalog.CreateTableOpt {
	order, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst,
	}})
	if err != nil {
		panic(err)
	}

	return catalog.WithSortOrder(order)
}
