using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Orleans.CodeGeneration;
using Orleans.Runtime;

namespace Orleans
{
    /// <summary>
    /// Factory for accessing grains.
    /// </summary>
    internal class GrainFactory : IInternalGrainFactory, IGrainReferenceConverter
    {
        /// <summary>
        /// The mapping between concrete grain interface types and delegate
        /// </summary>
        private readonly ConcurrentDictionary<Type, GrainReferenceCaster> casters
            = new ConcurrentDictionary<Type, GrainReferenceCaster>();

        /// <summary>
        /// The collection of <see cref="IGrainMethodInvoker"/>s for their corresponding grain interface type.
        /// </summary>
        private readonly ConcurrentDictionary<Type, IGrainMethodInvoker> invokers =
            new ConcurrentDictionary<Type, IGrainMethodInvoker>();

        /// <summary>
        /// The cache of typed system target references.
        /// </summary>
        private readonly Dictionary<Tuple<GrainId, Type>, Dictionary<SiloAddress, ISystemTarget>> typedSystemTargetReferenceCache =
                    new Dictionary<Tuple<GrainId, Type>, Dictionary<SiloAddress, ISystemTarget>>();

        /// <summary>
        /// The cache of type metadata.
        /// </summary>
        private readonly TypeMetadataCache typeCache;

        private readonly IRuntimeClient runtimeClient;

        private IGrainReferenceRuntime GrainReferenceRuntime => this.runtimeClient.GrainReferenceRuntime;

        // Make this internal so that client code is forced to access the IGrainFactory using the 
        // GrainClient (to make sure they don't forget to initialize the client).
        public GrainFactory(IRuntimeClient runtimeClient, TypeMetadataCache typeCache)
        {
            this.runtimeClient = runtimeClient;
            this.typeCache = typeCache;
        }

        /// <summary>
        /// Casts an <see cref="IAddressable"/> to a concrete <see cref="GrainReference"/> implementation.
        /// </summary>
        /// <param name="existingReference">The existing <see cref="IAddressable"/> reference.</param>
        /// <returns>The concrete <see cref="GrainReference"/> implementation.</returns>
        internal delegate object GrainReferenceCaster(IAddressable existingReference);

        /// <inheritdoc />
        public TGrainInterface GetGrain<TGrainInterface>(Guid primaryKey, string grainClassNamePrefix = null) where TGrainInterface : IGrainWithGuidKey
        {
            Type interfaceType = typeof(TGrainInterface);
            var implementation = this.GetGrainClassData(interfaceType, grainClassNamePrefix);
            var grainId = GrainId.GetGrainId(implementation.GetTypeCode(interfaceType), primaryKey, null);
            return this.Cast<TGrainInterface>(this.MakeGrainReferenceFromType(interfaceType, grainId));
        }

        /// <inheritdoc />
        public TGrainInterface GetGrain<TGrainInterface>(long primaryKey, string grainClassNamePrefix = null) where TGrainInterface : IGrainWithIntegerKey
        {
            Type interfaceType = typeof(TGrainInterface);
            var implementation = this.GetGrainClassData(interfaceType, grainClassNamePrefix);
            var grainId = GrainId.GetGrainId(implementation.GetTypeCode(interfaceType), primaryKey, null);
            return this.Cast<TGrainInterface>(this.MakeGrainReferenceFromType(interfaceType, grainId));
        }

        /// <inheritdoc />
        public TGrainInterface GetGrain<TGrainInterface>(string primaryKey, string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithStringKey
        {
            Type interfaceType = typeof(TGrainInterface);
            var implementation = this.GetGrainClassData(interfaceType, grainClassNamePrefix);
            var grainId = GrainId.GetGrainId(implementation.GetTypeCode(interfaceType), primaryKey);
            return this.Cast<TGrainInterface>(this.MakeGrainReferenceFromType(interfaceType, grainId));
        }


        /// <inheritdoc />
        public TGrainInterface GetGrain<TGrainInterface>(Guid primaryKey, string keyExtension, string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithGuidCompoundKey
        {
            GrainFactoryBase.DisallowNullOrWhiteSpaceKeyExtensions(keyExtension);

            Type interfaceType = typeof(TGrainInterface);
            var implementation = this.GetGrainClassData(interfaceType, grainClassNamePrefix);
            var grainId = GrainId.GetGrainId(implementation.GetTypeCode(interfaceType), primaryKey, keyExtension);
            return this.Cast<TGrainInterface>(this.MakeGrainReferenceFromType(interfaceType, grainId));
        }

        /// <inheritdoc />
        public TGrainInterface GetGrain<TGrainInterface>(long primaryKey, string keyExtension, string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithIntegerCompoundKey
        {
            GrainFactoryBase.DisallowNullOrWhiteSpaceKeyExtensions(keyExtension);

            Type interfaceType = typeof(TGrainInterface);
            var implementation = this.GetGrainClassData(interfaceType, grainClassNamePrefix);
            var grainId = GrainId.GetGrainId(implementation.GetTypeCode(interfaceType), primaryKey, keyExtension);
            return this.Cast<TGrainInterface>(this.MakeGrainReferenceFromType(interfaceType, grainId));
        }

        /// <inheritdoc />
        public void BindGrainReference(IAddressable grain)
        {
            if (grain == null) throw new ArgumentNullException(nameof(grain));
            var reference = grain as GrainReference;
            if (reference == null) throw new ArgumentException("Provided grain must be a GrainReference.", nameof(grain));
            reference.Bind(this.GrainReferenceRuntime);
        }

        /// <inheritdoc />
        public GrainReference GetGrainFromKeyString(string key) => GrainReference.FromKeyString(key, this.GrainReferenceRuntime);

        /// <inheritdoc />
        public Task<TGrainObserverInterface> CreateObjectReference<TGrainObserverInterface>(IGrainObserver obj)
            where TGrainObserverInterface : IGrainObserver
        {
            return Task.FromResult(this.CreateObjectReferenceImpl<TGrainObserverInterface>(obj));
        }

        /// <inheritdoc />
        public Task DeleteObjectReference<TGrainObserverInterface>(
            IGrainObserver obj) where TGrainObserverInterface : IGrainObserver
        {
            this.runtimeClient.DeleteObjectReference(obj);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public TGrainObserverInterface CreateObjectReference<TGrainObserverInterface>(IAddressable obj)
                where TGrainObserverInterface : IAddressable
        {
            return this.CreateObjectReferenceImpl<TGrainObserverInterface>(obj);
        }

        private TGrainObserverInterface CreateObjectReferenceImpl<TGrainObserverInterface>(IAddressable obj) where TGrainObserverInterface : IAddressable
        {
            var interfaceType = typeof(TGrainObserverInterface);
            if (!interfaceType.IsInterface)
            {
                throw new ArgumentException(
                    $"The provided type parameter must be an interface. '{interfaceType.FullName}' is not an interface.");
            }

            if (!interfaceType.IsInstanceOfType(obj))
            {
                throw new ArgumentException($"The provided object must implement '{interfaceType.FullName}'.", nameof(obj));
            }

            IGrainMethodInvoker invoker;
            if (!this.invokers.TryGetValue(interfaceType, out invoker))
            {
                invoker = this.MakeInvoker(interfaceType);
                this.invokers.TryAdd(interfaceType, invoker);
            }

            return this.Cast<TGrainObserverInterface>(this.runtimeClient.CreateObjectReference(obj, invoker));
        }

        internal IAddressable MakeGrainReferenceFromType(Type interfaceType, GrainId grainId)
        {
            return GrainReference.FromGrainId(
                grainId,
                this.GrainReferenceRuntime,
                interfaceType.IsGenericType ? TypeUtils.GenericTypeArgsString(interfaceType.UnderlyingSystemType.FullName) : null);
        }

        private GrainClassData GetGrainClassData(Type interfaceType, string grainClassNamePrefix)
        {
            if (!GrainInterfaceUtils.IsGrainType(interfaceType))
            {
                throw new ArgumentException("Cannot fabricate grain-reference for non-grain type: " + interfaceType.FullName);
            }

            var grainTypeResolver = this.runtimeClient.GrainTypeResolver;
            GrainClassData implementation;
            if (!grainTypeResolver.TryGetGrainClassData(interfaceType, out implementation, grainClassNamePrefix))
            {
                var loadedAssemblies = grainTypeResolver.GetLoadedGrainAssemblies();
                var assembliesString = string.IsNullOrEmpty(loadedAssemblies)
                    ? string.Empty
                    : " Loaded grain assemblies: " + loadedAssemblies;
                var grainClassPrefixString = string.IsNullOrEmpty(grainClassNamePrefix)
                    ? string.Empty
                    : ", grainClassNamePrefix: " + grainClassNamePrefix;
                throw new ArgumentException(
                    $"Cannot find an implementation class for grain interface: {interfaceType}{grainClassPrefixString}. " +
                    "Make sure the grain assembly was correctly deployed and loaded in the silo." + assembliesString);
            }

            return implementation;
        }

        private IGrainMethodInvoker MakeInvoker(Type interfaceType)
        {
            var invokerType = this.typeCache.GetGrainMethodInvokerType(interfaceType);
            return (IGrainMethodInvoker)Activator.CreateInstance(invokerType);
        }

        /// <summary>
        /// Casts the provided <paramref name="grain"/> to the specified interface
        /// </summary>
        /// <typeparam name="TGrainInterface">The target grain interface type.</typeparam>
        /// <param name="grain">The grain reference being cast.</param>
        /// <returns>
        /// A reference to <paramref name="grain"/> which implements <typeparamref name="TGrainInterface"/>.
        /// </returns>
        public TGrainInterface Cast<TGrainInterface>(IAddressable grain)
        {
            var interfaceType = typeof(TGrainInterface);
            return (TGrainInterface)this.Cast(grain, interfaceType);
        }

        /// <summary>
        /// Casts the provided <paramref name="grain"/> to the provided <paramref name="interfaceType"/>.
        /// </summary>
        /// <param name="grain">The grain.</param>
        /// <param name="interfaceType">The resulting interface type.</param>
        /// <returns>A reference to <paramref name="grain"/> which implements <paramref name="interfaceType"/>.</returns>
        public object Cast(IAddressable grain, Type interfaceType)
        {
            GrainReferenceCaster caster;
            if (!this.casters.TryGetValue(interfaceType, out caster))
            {
                // Create and cache a caster for the interface type.
                caster = this.casters.GetOrAdd(interfaceType, this.MakeCaster);
            }

            return caster(grain);
        }

        /// <summary>
        /// Creates and returns a new grain reference caster.
        /// </summary>
        /// <param name="interfaceType">The interface which the result will cast to.</param>
        /// <returns>A new grain reference caster.</returns>
        private GrainReferenceCaster MakeCaster(Type interfaceType)
        {
            var grainReferenceType = this.typeCache.GetGrainReferenceType(interfaceType);
            return GrainCasterFactory.CreateGrainReferenceCaster(interfaceType, grainReferenceType);
        }

        /// <summary>
        /// Gets a reference to the specified system target.
        /// </summary>
        /// <typeparam name="TGrainInterface">The system target interface.</typeparam>
        /// <param name="grainId">The id of the target.</param>
        /// <param name="destination">The destination silo.</param>
        /// <returns>A reference to the specified system target.</returns>
        public TGrainInterface GetSystemTarget<TGrainInterface>(GrainId grainId, SiloAddress destination)
            where TGrainInterface : ISystemTarget
        {
            Dictionary<SiloAddress, ISystemTarget> cache;
            Tuple<GrainId, Type> key = Tuple.Create(grainId, typeof(TGrainInterface));

            lock (this.typedSystemTargetReferenceCache)
            {
                if (this.typedSystemTargetReferenceCache.ContainsKey(key)) cache = this.typedSystemTargetReferenceCache[key];
                else
                {
                    cache = new Dictionary<SiloAddress, ISystemTarget>();
                    this.typedSystemTargetReferenceCache[key] = cache;
                }
            }

            ISystemTarget reference;
            lock (cache)
            {
                if (cache.ContainsKey(destination))
                {
                    reference = cache[destination];
                }
                else
                {
                    reference = this.Cast<TGrainInterface>(GrainReference.FromGrainId(grainId, this.GrainReferenceRuntime, null, destination));
                    cache[destination] = reference; // Store for next time
                }
            }

            return (TGrainInterface)reference;
        }

        /// <inheritdoc />
        public TGrainInterface GetGrain<TGrainInterface>(GrainId grainId) where TGrainInterface : IAddressable
        {
            return this.Cast<TGrainInterface>(GrainReference.FromGrainId(grainId, this.GrainReferenceRuntime));
        }

        /// <inheritdoc />
        public GrainReference GetGrain(GrainId grainId, string genericArguments)
            => GrainReference.FromGrainId(grainId, this.GrainReferenceRuntime, genericArguments);


        /// <summary>
        /// A GetGrain overload that returns the runtime type of the grain interface and returns the grain cast to
        /// TGrainInterface.
        /// 
        /// The main use-case is when you want to get a grain whose type is unknown at compile time (e.g. generic type parameters).
        /// </summary>
        /// <typeparam name="TGrainInterface">The output type of the grain</typeparam>
        /// <param name="grainPrimaryKey">the primary key of the grain</param>
        /// <param name="grainInterfaceType">the runtime type of the grain interface</param>
        /// <returns>the requested grain with the given grainID and grainInterfaceType</returns>
        public TGrainInterface GetGrain<TGrainInterface>(Guid grainPrimaryKey, Type grainInterfaceType)
            where TGrainInterface : IGrain
        {
            this.runtimeClient.GrainTypeResolver.TryGetGrainClassData(grainInterfaceType, out GrainClassData implementation, string.Empty);
            var grainId = GrainId.GetGrainId(implementation.GetTypeCode(grainInterfaceType), grainPrimaryKey);
            return this.Cast<TGrainInterface>(this.MakeGrainReferenceFromType(grainInterfaceType, grainId));
        }

        /// <summary>
        /// A GetGrain overload that returns the runtime type of the grain interface and returns the grain cast to
        /// <see paramref="TGrainInterface"/>. It is the caller's responsibility to ensure <see paramref="TGrainInterface"/>
        /// extends IGrain, as there is no compile-time checking for this overload.
        /// 
        /// The main use-case is when you want to get a grain whose type is unknown at compile time.
        /// </summary>
        /// <param name="grainPrimaryKey">the primary key of the grain</param>
        /// <param name="grainInterfaceType">the runtime type of the grain interface</param>
        /// <param name="TGrainInterface">the type of grain interface that should be returned</param>
        /// <returns></returns>
        public IGrain GetGrain(string grainPrimaryKey, Type grainInterfaceType, Type TGrainInterface)
        {
            this.runtimeClient.GrainTypeResolver.TryGetGrainClassData(grainInterfaceType, out GrainClassData implementation, string.Empty);
            var grainId = GrainId.GetGrainId(implementation.GetTypeCode(grainInterfaceType), grainPrimaryKey);
            return (IGrain)this.Cast(this.MakeGrainReferenceFromType(grainInterfaceType, grainId), TGrainInterface);
        }
    }
}
