using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Indexing.Facet
{
    abstract class IndexWriterAttributeMapperBase
    {
        public Factory<IGrainActivationContext, object> GetFactory(MethodInfo creator, ParameterInfo parameter, IIndexWriterConfiguration config)
        {
            // Use generic type args to specialize the generic method and create the factory lambda.
            var genericCreate = creator.MakeGenericMethod(parameter.ParameterType.GetGenericArguments());
            var args = new object[] { config };
            return context => this.Create(context, genericCreate, args);
        }

        private object Create(IGrainActivationContext context, MethodInfo genericCreate, object[] args)
        {
            var factory = context.ActivationServices.GetRequiredService<IIndexWriterFactory>();
            return genericCreate.Invoke(factory, args);
        }
    }
}
