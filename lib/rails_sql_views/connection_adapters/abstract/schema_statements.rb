module RailsSqlViews
  module ConnectionAdapters # :nodoc:
    module SchemaStatements

      VIRTUAL_TABLE_MAPPING = { :view => "VIEW", :materialized_view => "MATERIALIZED VIEW" }
      # Create a view.
      # The +options+ hash can include the following keys:
      # [<tt>:check_option</tt>]
      #   Specify restrictions for inserts or updates in updatable views. ANSI SQL 92 defines two check option
      #   values: CASCADED and LOCAL. See your database documentation for allowed values.


      def create_function(name, function_definition, options= { })
        if supports_functions?
          params = ParamDefinition.new(self, options[:return])

          if block_given?
            yield params
          end

          create_sql = "CREATE OR REPLACE FUNCTION #{quote_table_name(name)}"
          create_sql << params.to_sql
          create_sql << "\n IS \n"
          create_sql << function_definition

          execute create_sql
        end
      end

      def create_view(name, select_query, options={ }, &block)
        create_virtual_table(name, select_query, :view, options, &block)
      end

      def create_materialized_view(name, select_query, options={ }, &block)
        create_virtual_table(name, select_query, :materialized_view, options, &block)
      end

      def create_virtual_table(name, select_query, type, options)
        if send("supports_#{type}s?".to_sym)
          view_definition = ViewDefinition.new(self, select_query)

          if block_given?
            yield view_definition
          end

          if options[:force]
            send("drop_#{type}".to_sym, name) rescue nil
          end

          create_sql = "CREATE #{VIRTUAL_TABLE_MAPPING[type]} "
          create_sql << "#{quote_table_name(name)} "
          if supports_view_columns_definition? && !view_definition.to_sql.blank?
            create_sql << "("
            create_sql << view_definition.to_sql
            create_sql << ") "
          end
          create_sql << "AS #{view_definition.select_query}"
          create_sql << " WITH #{options[:check_option]} CHECK OPTION" if options[:check_option]
          execute create_sql
        end
      end

      # Also creates a view, with the specific purpose of remapping column names
      # to make non-ActiveRecord tables friendly with the naming
      # conventions, while maintaining legacy app compatibility.
      def create_mapping_view(old_name, new_name, options = { })
        return unless supports_views?

        col_names = columns(old_name).collect { |col| col.name.to_sym }
        mapper    = MappingDefinition.new(col_names)

        yield mapper

        if options[:force]
          drop_view(new_name) rescue nil
        end

        view_sql = "CREATE VIEW #{new_name} "
        if supports_view_columns_definition?
          view_sql << "(#{mapper.view_cols.collect { |c| quote_column_name(c) }.join(', ')}) "
        end
        view_sql << "AS SELECT #{mapper.select_cols.collect { |c| quote_column_name(c) }.join(', ')} FROM #{old_name}"
        execute view_sql
      end

      def drop_table_with_cascade(table_name, options = { })
        execute "DROP TABLE #{quote_table_name(table_name)} CASCADE CONSTRAINTS"
      end

      # Drop a view.
      # The +options+ hash can include the following keys:
      # [<tt>:drop_behavior</tt>]
      #   Specify the drop behavior. ANSI SQL 92 defines two drop behaviors, CASCADE and RESTRICT. See your
      #   database documentation to determine what drop behaviors are available.
      def drop_view(name, options={ })
        drop_virtual_table(name, :view, options)
      end

      # Drop a materialized view.
      def drop_materialized_view(name, options={ })
        drop_virtual_table(name, :materialized_view, options)
      end

      def drop_virtual_table(name, type, options)
        if send("supports_#{type}s?".to_sym)
          drop_sql = "DROP #{VIRTUAL_TABLE_MAPPING[type]} #{quote_table_name(name)}"
          drop_sql << " #{options[:drop_behavior]}" if options[:drop_behavior]
          execute drop_sql
        end
      end
    end
  end
end